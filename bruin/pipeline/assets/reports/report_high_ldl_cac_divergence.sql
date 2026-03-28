/* @bruin
name: reports.high_ldl_cac_divergence
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_cac_health_screening

columns:
  - name: cohort
    type: string
    description: "Protected (CAC=0) vs Calcified (CAC>0)"
    checks:
      - name: not_null
  - name: subject_count
    type: integer
    description: "Number of subjects in this cohort"
    checks:
      - name: not_null
@bruin */

-- =============================================================================
-- REPORT: What differentiates metabolically healthy + high-LDL people
--         who have ZERO calcification from those who DO calcify?
-- =============================================================================
-- Population: Metabolically healthy (met_score = 0) with Borderline high, High, or Very High LDL
-- Split:
--   "Protected"  = CAC = 0  (high LDL but clean arteries — the "paradox")
--   "Calcified"  = CAC > 0  (high LDL and calcification present)
--
-- If LDL alone drove calcification, these two groups should look identical
-- on every other biomarker. Any significant difference reveals the REAL
-- risk factors hiding behind the LDL number.
-- =============================================================================

SELECT
    CASE WHEN cacs = 0 THEN 'Protected (CAC=0)'
         ELSE 'Calcified (CAC>0)'
    END                                                         AS cohort,

    -- Group size
    COUNT(*)                                                    AS subject_count,

    -- Demographics
    ROUND(AVG(age_yr), 1)                                      AS avg_age,
    ROUND(100.0 * SUM(CASE WHEN sex = 'M' THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_male,
    ROUND(AVG(bmi), 1)                                         AS avg_bmi,

    -- CAC severity (for context)
    ROUND(AVG(cacs), 2)                                        AS avg_cacs,
    ROUND(MEDIAN(cacs), 2)                                     AS median_cacs,

    -- Lipid panel
    ROUND(AVG(ldl_chol), 1)                                    AS avg_ldl,
    ROUND(AVG(chol), 1)                                        AS avg_total_cholesterol,
    ROUND(AVG(hdl_chol), 1)                                    AS avg_hdl,
    ROUND(AVG(tg), 1)                                          AS avg_triglycerides,
    ROUND(AVG(non_hdl_chol), 1)                                AS avg_non_hdl_chol,
    ROUND(AVG(tg_hdl_ratio), 2)                                AS avg_tg_hdl_ratio,

    -- LDL particle pattern distribution (% Pattern B = small dense)
    ROUND(100.0 * SUM(CASE WHEN estimated_ldl_particle_pattern = 'Pattern B (Small Dense)'
        THEN 1 ELSE 0 END) / COUNT(*), 1)                     AS pct_pattern_b,

    -- Metabolic markers
    ROUND(AVG(glucose), 1)                                     AS avg_glucose,
    ROUND(AVG(hb_a1c), 2)                                     AS avg_hba1c,
    ROUND(AVG(sbp), 1)                                         AS avg_sbp,
    ROUND(AVG(dbp), 1)                                         AS avg_dbp,

    -- Inflammation
    ROUND(AVG(hs_crp), 2)                                     AS avg_hs_crp,

    -- Kidney function
    ROUND(AVG(creatinine), 2)                                  AS avg_creatinine,
    ROUND(AVG(idms_mdrd_gfr), 1)                               AS avg_gfr,

    -- Smoking
    ROUND(100.0 * SUM(CASE WHEN smoking_status = 'Current' THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_current_smoker,
    ROUND(100.0 * SUM(CASE WHEN smoking_status = 'Former' THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_ex_smoker

FROM staging.stg_cac_health_screening
WHERE metabolic_health_status = 'Metabolically healthy'
  AND ldl_category IN ('Borderline high', 'High', 'Very high')
GROUP BY cohort
ORDER BY cohort
