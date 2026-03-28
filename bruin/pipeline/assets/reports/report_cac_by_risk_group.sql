/* @bruin
name: reports.cac_by_risk_group
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_cac_health_screening

columns:
  - name: ldl_category
    type: string
    description: "LDL classification bucket"
    checks:
      - name: not_null
  - name: metabolic_health_status
    type: string
    description: "Metabolically healthy vs unhealthy"
    checks:
      - name: not_null
  - name: subject_count
    type: integer
    description: "Number of subjects in this group"
    checks:
      - name: not_null
@bruin */

-- =============================================================================
-- REPORT: CAC outcomes by LDL category x Metabolic health status
-- =============================================================================
-- This is the core thesis table. It answers:
--   "Does metabolic health predict CAC severity better than LDL level?"
--
-- Dashboard tile: grouped bar chart or heatmap
--   X-axis: ldl_category
--   Color/group: metabolic_health_status
--   Values: avg_cacs, pct_any_calcification, pct_moderate_or_severe
--
-- The four-quadrant comparison lives here:
--   Group A: High LDL + Metabolically healthy    → expect LOW CAC if myth is wrong
--   Group B: High LDL + Metabolically unhealthy  → expect HIGH CAC
--   Group C: Low LDL + Metabolically healthy      → expect LOW CAC
--   Group D: Low LDL + Metabolically unhealthy    → expect HIGH CAC if thesis holds
-- =============================================================================

SELECT
    ldl_category,
    metabolic_health_status,

    -- Group size
    COUNT(*)                                                    AS subject_count,

    -- CAC averages
    ROUND(AVG(cacs), 2)                                        AS avg_cacs,
    ROUND(MEDIAN(cacs), 2)                                     AS median_cacs,

    -- CAC severity distribution (percentages)
    ROUND(100.0 * SUM(CASE WHEN cacs > 0 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_any_calcification,

    ROUND(100.0 * SUM(CASE WHEN cacs >= 100 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                         AS pct_moderate_or_severe,

    -- Metabolic context (averages per group for tooltip/detail)
    ROUND(AVG(tg), 1)                                          AS avg_triglycerides,
    ROUND(AVG(glucose), 1)                                     AS avg_glucose,
    ROUND(AVG(hdl_chol), 1)                                    AS avg_hdl,
    ROUND(AVG(hb_a1c), 2)                                     AS avg_hba1c,
    ROUND(AVG(tg_hdl_ratio), 2)                                AS avg_tg_hdl_ratio

FROM staging.stg_cac_health_screening
GROUP BY ldl_category, metabolic_health_status
ORDER BY ldl_category, metabolic_health_status
