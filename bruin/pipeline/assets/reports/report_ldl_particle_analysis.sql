/* @bruin
name: reports.ldl_particle_analysis
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_cac_health_screening

columns:
  - name: estimated_ldl_particle_pattern
    type: string
    description: "Pattern A (Large Buoyant) vs Pattern B (Small Dense)"
  - name: subject_count
    type: integer
    description: "Number of subjects in this group"
    checks:
      - name: not_null
@bruin */

-- =============================================================================
-- REPORT: LDL particle pattern impact on CAC
-- =============================================================================
-- Dashboard tile: side-by-side comparison or stacked bar
--   Groups: Pattern A vs Pattern B, split by LDL category
--   Values: avg_cacs, pct_any_calcification
--
-- This answers: "At the SAME LDL level, do people with small dense LDL
-- (Pattern B) have worse CAC than those with large buoyant LDL (Pattern A)?"
-- If yes, it proves LDL quantity alone is misleading — quality matters.
-- =============================================================================

SELECT
    estimated_ldl_particle_pattern,
    ldl_category,

    -- Group size
    COUNT(*)                                                    AS subject_count,

    -- CAC metrics
    ROUND(AVG(cacs), 2)                                        AS avg_cacs,
    ROUND(MEDIAN(cacs), 2)                                     AS median_cacs,

    ROUND(100.0 * SUM(CASE WHEN cacs > 0 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_any_calcification,

    ROUND(100.0 * SUM(CASE WHEN cacs >= 100 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_moderate_or_severe,

    -- Context
    ROUND(AVG(tg_hdl_ratio), 2)                                AS avg_tg_hdl_ratio,
    ROUND(AVG(non_hdl_chol), 1)                                AS avg_non_hdl_chol

FROM staging.stg_cac_health_screening
WHERE estimated_ldl_particle_pattern IS NOT NULL
GROUP BY estimated_ldl_particle_pattern, ldl_category
ORDER BY estimated_ldl_particle_pattern, ldl_category
