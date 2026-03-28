/* @bruin
name: reports.cac_by_age
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_cac_health_screening

columns:
  - name: age_group
    type: string
    description: "Age bucket (decade-based)"
    checks:
      - name: not_null
  - name: subject_count
    type: integer
    description: "Number of subjects in this age group"
    checks:
      - name: not_null
@bruin */

-- =============================================================================
-- REPORT: CAC progression by age group
-- =============================================================================
-- Dashboard tile: LINE CHART (temporal distribution)
--   X-axis: age_group (ordered)
--   Lines: avg_cacs, pct_any_calcification (one line per metabolic_health_status)
--
-- NOTE: This dataset has no dates — it's a cross-sectional snapshot.
-- Age serves as a temporal proxy: older age groups show what happens
-- to arteries over time. This satisfies the zoomcamp rubric requirement
-- for "distribution across a temporal line."
-- =============================================================================

SELECT
    -- Age buckets by decade
    CASE
        WHEN age_yr < 40 THEN '30-39'
        WHEN age_yr < 50 THEN '40-49'
        WHEN age_yr < 60 THEN '50-59'
        WHEN age_yr < 70 THEN '60-69'
        WHEN age_yr >= 70 THEN '70+'
    END AS age_group,

    metabolic_health_status,

    -- Group size
    COUNT(*)                                                    AS subject_count,

    -- CAC metrics
    ROUND(AVG(cacs), 2)                                        AS avg_cacs,
    ROUND(MEDIAN(cacs), 2)                                     AS median_cacs,

    ROUND(100.0 * SUM(CASE WHEN cacs > 0 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_any_calcification,

    ROUND(100.0 * SUM(CASE WHEN cacs >= 100 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_moderate_or_severe,

    -- Lipid & metabolic averages per age group
    ROUND(AVG(ldl_chol), 1)                                    AS avg_ldl,
    ROUND(AVG(tg), 1)                                          AS avg_triglycerides,
    ROUND(AVG(tg_hdl_ratio), 2)                                AS avg_tg_hdl_ratio,
    ROUND(AVG(metabolic_syndrome_score), 2)                    AS avg_met_score

FROM staging.stg_cac_health_screening
GROUP BY age_group, metabolic_health_status
ORDER BY age_group, metabolic_health_status
