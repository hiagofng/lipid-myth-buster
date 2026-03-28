/* @bruin
name: reports.pure_high_ldl_cac
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_cac_health_screening

columns:
  - name: stage
    type: string
    description: "Funnel stage or CAC severity bucket"
    checks:
      - name: not_null
  - name: subject_count
    type: integer
    description: "Number of subjects at this stage"
    checks:
      - name: not_null
@bruin */

-- =============================================================================
-- REPORT: The "pure" LDL test
-- =============================================================================
-- Progressive funnel: start with elevated LDL, strip away every known
-- confounder (metabolic syndrome, smoking, kidney dysfunction), then show
-- the CAC severity distribution of whoever remains.
--
-- If LDL alone drives calcification, the final cohort should still show
-- significant CAC. If it doesn't — the myth is busted.
-- =============================================================================

-- Part 1: Funnel counts
WITH base AS (
    SELECT * FROM staging.stg_cac_health_screening
),
funnel AS (
    SELECT
        'All subjects in dataset'                                   AS stage, 1 AS stage_order, COUNT(*) AS subject_count
    FROM base
    UNION ALL
    SELECT
        'High LDL only (≥130 mg/dL)'                               AS stage, 2, COUNT(*)
    FROM base WHERE ldl_category IN ('Borderline high', 'High', 'Very high')
    UNION ALL
    SELECT
        'High LDL + metabolically healthy'                          AS stage, 3, COUNT(*)
    FROM base WHERE ldl_category IN ('Borderline high', 'High', 'Very high')
      AND metabolic_health_status = 'Metabolically healthy'
    UNION ALL
    SELECT
        'High LDL + healthy + never smoked'                         AS stage, 4, COUNT(*)
    FROM base WHERE ldl_category IN ('Borderline high', 'High', 'Very high')
      AND metabolic_health_status = 'Metabolically healthy'
      AND smoking_status = 'Never'
    UNION ALL
    SELECT
        'High LDL + healthy + non-smoker + normal kidneys'          AS stage, 5, COUNT(*)
    FROM base WHERE ldl_category IN ('Borderline high', 'High', 'Very high')
      AND metabolic_health_status = 'Metabolically healthy'
      AND smoking_status = 'Never'
      AND idms_mdrd_gfr >= 90
),

-- Part 2: CAC severity breakdown for the final filtered cohort
cac_dist AS (
    SELECT
        'CAC: ' || cac_risk_category  AS stage,
        CASE cac_risk_category
            WHEN 'No calcification' THEN 6
            WHEN 'Mild'             THEN 7
            WHEN 'Moderate'         THEN 8
            WHEN 'Severe'           THEN 9
        END                           AS stage_order,
        COUNT(*)                      AS subject_count
    FROM base
    WHERE ldl_category IN ('Borderline high', 'High', 'Very high')
      AND metabolic_health_status = 'Metabolically healthy'
      AND smoking_status = 'Never'
      AND idms_mdrd_gfr >= 90
    GROUP BY cac_risk_category
)

SELECT stage, stage_order, subject_count FROM funnel
UNION ALL
SELECT stage, stage_order, subject_count FROM cac_dist
ORDER BY stage_order
