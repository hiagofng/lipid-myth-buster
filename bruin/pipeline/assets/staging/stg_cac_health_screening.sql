/* @bruin
name: staging.stg_cac_health_screening
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - ingestion.cac_health_screening

columns:
  - name: no_figshare
    type: integer
    description: "Subject ID (unique per participant)"
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: cacs
    type: float
    description: "Coronary Artery Calcium Score"
    checks:
      - name: not_null
  - name: cac_risk_category
    type: string
    description: "CAC risk level: No calcification / Mild / Moderate / Severe"
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "No calcification"
          - "Mild"
          - "Moderate"
          - "Severe"
          - "Unknown"
  - name: ldl_category
    type: string
    description: "LDL classification per ATP III guidelines"
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "Optimal"
          - "Near optimal"
          - "Borderline high"
          - "High"
          - "Very high"
          - "Unknown"
  - name: smoking_status
    type: string
    description: "Derived from ex_smoker + current_smoker flags"
    checks:
      - name: accepted_values
        value:
          - "Current"
          - "Former"
          - "Never"
  - name: estimated_ldl_particle_pattern
    type: string
    description: "Proxy for LDL particle size based on TG/HDL ratio (Pattern A = large buoyant, Pattern B = small dense). Note: this is an estimate, not a direct measurement."
    checks:
      - name: accepted_values
        value:
          - "Pattern A (Large Buoyant)"
          - "Pattern B (Small Dense)"
  - name: tg_hdl_ratio
    type: float
    description: "TG/HDL ratio — proxy for LDL particle size (>2.5 suggests small dense LDL)"
  - name: non_hdl_chol
    type: float
    description: "Non-HDL cholesterol (Total - HDL) — captures LDL + VLDL + IDL per AHA/ACC guidelines"
  - name: metabolic_syndrome_score
    type: integer
    description: "Count of MetS criteria met (0-5). Uses BMI>=30 as proxy for waist circumference (not available in dataset). Criteria: high TG, low HDL, high glucose, obesity, hypertension."
  - name: metabolic_health_status
    type: string
    description: "Metabolically healthy (0 flags) vs unhealthy (1+ flags). Useful for isolating high-LDL-but-healthy vs low-LDL-but-unhealthy groups."
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "Metabolically healthy"
          - "Metabolically unhealthy"
@bruin */

-- =============================================================================
-- STAGING: stg_cac_health_screening
-- =============================================================================
-- Transforms raw ingestion data into analysis-ready columns for LipidMythBuster.
--
-- What this does:
--   1. Passes through raw biomarker values unchanged
--   2. Derives clinical risk categories (CAC, LDL, smoking)
--   3. Computes metabolic risk flags and syndrome score
--   4. Estimates LDL particle pattern via TG/HDL ratio
--   5. Calculates non-HDL cholesterol (AHA/ACC recommended)
--
-- Source: ingestion.cac_health_screening (1,688 rows, Korean CAC study)
-- =============================================================================

WITH base AS (
    SELECT
        *,

        -- Metabolic risk flags (computed once in CTE, reused in final SELECT)
        -- Each flag is 0 or 1 based on standard clinical thresholds.
        CASE WHEN tg >= 150 THEN 1 ELSE 0 END                          AS _flag_high_tg,
        CASE WHEN glucose >= 100 THEN 1 ELSE 0 END                     AS _flag_high_glucose,
        CASE
            WHEN sex = 'M' AND hdl_chol < 40 THEN 1
            WHEN sex = 'F' AND hdl_chol < 50 THEN 1
            ELSE 0
        END                                                              AS _flag_low_hdl,
        CASE WHEN bmi >= 30 THEN 1 ELSE 0 END                          AS _flag_obese,
        CASE WHEN hypertension = 1 THEN 1 ELSE 0 END                   AS _flag_hypertension

    FROM ingestion.cac_health_screening
    WHERE no_figshare IS NOT NULL
)

SELECT
    -- =====================================================================
    -- 1. PASS-THROUGH: Original columns kept as-is
    -- =====================================================================
    no_figshare,
    age_yr,
    sex,
    sbp,
    dbp,
    weight,
    bmi,
    glucose,
    chol,
    creatinine,
    idms_mdrd_gfr,
    tg,
    hdl_chol,
    ldl_chol,
    hs_crp,
    hb_a1c,
    hypertension,
    diabetes,
    cacs,

    -- =====================================================================
    -- 2. DERIVED: CAC risk category (standard clinical cutoffs)
    --    Source: Multi-Ethnic Study of Atherosclerosis (MESA) guidelines
    -- =====================================================================
    CASE
        WHEN cacs = 0                  THEN 'No calcification'
        WHEN cacs BETWEEN 1 AND 99     THEN 'Mild'
        WHEN cacs BETWEEN 100 AND 399  THEN 'Moderate'
        WHEN cacs >= 400               THEN 'Severe'
        ELSE 'Unknown'
    END AS cac_risk_category,

    -- =====================================================================
    -- 3. DERIVED: LDL cholesterol category (ATP III guidelines)
    --    Source: NCEP Adult Treatment Panel III
    -- =====================================================================
    CASE
        WHEN ldl_chol < 100  THEN 'Optimal'
        WHEN ldl_chol < 130  THEN 'Near optimal'
        WHEN ldl_chol < 160  THEN 'Borderline high'
        WHEN ldl_chol < 190  THEN 'High'
        WHEN ldl_chol >= 190 THEN 'Very high'
        ELSE 'Unknown'
    END AS ldl_category,

    -- =====================================================================
    -- 4. DERIVED: Individual metabolic risk flags (binary 0/1)
    -- =====================================================================
    _flag_high_tg           AS high_triglycerides,
    _flag_high_glucose      AS high_glucose,
    _flag_low_hdl           AS low_hdl,
    _flag_obese             AS obese,
    CASE WHEN hb_a1c >= 5.7 THEN 1 ELSE 0 END AS prediabetic_or_diabetic,

    -- =====================================================================
    -- 5. DERIVED: Smoking status (merge two binary columns into one)
    -- =====================================================================
    CASE
        WHEN current_smoker = 1 THEN 'Current'
        WHEN ex_smoker = 1      THEN 'Former'
        ELSE 'Never'
    END AS smoking_status,

    -- =====================================================================
    -- 6. DERIVED: TG/HDL ratio & estimated LDL particle pattern
    --    Ref: Maruyama et al. (2003), Boizel et al. (2000)
    --    Ratio > 2.5 suggests predominance of small dense LDL (Pattern B).
    --    NOTE: This is a population-level proxy, not a direct NMR measurement.
    -- =====================================================================
    ROUND(tg / NULLIF(hdl_chol, 0), 2) AS tg_hdl_ratio,

    CASE
        WHEN (tg / NULLIF(hdl_chol, 0)) > 2.5 THEN 'Pattern B (Small Dense)'
        WHEN (tg / NULLIF(hdl_chol, 0)) IS NOT NULL THEN 'Pattern A (Large Buoyant)'
        ELSE NULL
    END AS estimated_ldl_particle_pattern,

    -- =====================================================================
    -- 7. DERIVED: Non-HDL Cholesterol (Total Chol - HDL)
    --    Captures LDL + VLDL + IDL — recommended by AHA/ACC as superior
    --    to LDL-C alone, especially when triglycerides are elevated.
    -- =====================================================================
    (chol - hdl_chol) AS non_hdl_chol,

    -- =====================================================================
    -- 8. DERIVED: Metabolic Syndrome Score (0-5)
    --    Based on NCEP ATP III criteria, adapted:
    --      - BMI >= 30 used as proxy for waist circumference (not in dataset)
    --      - Hypertension flag from source data (binary)
    --    A score >= 3 meets clinical MetS diagnosis threshold.
    -- =====================================================================
    (_flag_high_tg + _flag_high_glucose + _flag_low_hdl + _flag_obese + _flag_hypertension)
        AS metabolic_syndrome_score,

    -- =====================================================================
    -- 9. DERIVED: Metabolic health status (binary grouping)
    --    Key column for the core thesis: enables direct comparison of
    --    "high LDL + metabolically healthy" vs "low LDL + metabolically unhealthy"
    --    Uses the 5 MetS flags; healthy = zero flags triggered.
    -- =====================================================================
    CASE
        WHEN (_flag_high_tg + _flag_high_glucose + _flag_low_hdl + _flag_obese + _flag_hypertension) = 0
            THEN 'Metabolically healthy'
        ELSE 'Metabolically unhealthy'
    END AS metabolic_health_status,

    -- =====================================================================
    -- 10. LINEAGE: carry forward ingestion timestamp
    -- =====================================================================
    extracted_at

FROM base
