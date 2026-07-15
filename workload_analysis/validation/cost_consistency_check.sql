-- =====================================================================
-- Cost consistency validation: comparison before/after right_sizing_analysis_mv split
-- =====================================================================
-- Verify that the sum of total_dbus, total_cost_usd of
-- all_purpose_cluster_sizing_mv (MV 12) + job_compute_sizing_mv (MV 13)
-- matches the source before the split.
--
-- Validation principle:
--   job_run_cost_analysis_mv (is_serverless=false) sum
--   == MV12 sum + MV13 sum
--
-- How to run:
--   After deploying the pipeline, run the queries below in order and confirm result = 'PASS'
--
-- Parameters:
--   ${source_catalog}   - pipeline target catalog (e.g. hurcy)
--   ${analytics_schema} - pipeline target schema  (e.g. test)
-- =====================================================================


-- =============================================================
-- Check 1: source baseline — job_run_cost_analysis_mv (classic compute)
-- =============================================================
-- Common baseline before/after the split. Total sum of is_serverless=false.
-- =============================================================
SELECT
  'source_baseline'             AS check_name,
  ROUND(SUM(total_dbus), 4)     AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
WHERE is_serverless = false;


-- =============================================================
-- Check 2: sum after split — MV12 (All-Purpose) + MV13 (Job Compute)
-- =============================================================
-- Since it is split by cluster_source filter, the sum must cover the whole.
-- =============================================================
SELECT
  'after_split'                 AS check_name,
  ROUND(SUM(total_dbus), 4)     AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
FROM (
  SELECT total_dbus, total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
  UNION ALL
  SELECT total_dbus, total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
);


-- =============================================================
-- Check 3: verify difference — PASS / FAIL determination
-- =============================================================
-- PASS if the cost difference is less than 0.01.
-- On FAIL, check whether cluster_source has values other than 'UI', 'API', 'JOB'.
-- =============================================================
WITH baseline AS (
  SELECT
    SUM(total_dbus)     AS dbus,
    SUM(total_cost_usd) AS cost
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
  WHERE is_serverless = false
),
split_total AS (
  SELECT
    SUM(total_dbus)     AS dbus,
    SUM(total_cost_usd) AS cost
  FROM (
    SELECT total_dbus, total_cost_usd
    FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
    UNION ALL
    SELECT total_dbus, total_cost_usd
    FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
  )
)
SELECT
  ROUND(baseline.dbus,  4)                          AS baseline_dbus,
  ROUND(split_total.dbus,  4)                       AS split_dbus,
  ROUND(ABS(baseline.dbus  - split_total.dbus),  4) AS dbus_diff,
  ROUND(baseline.cost,  2)                          AS baseline_cost_usd,
  ROUND(split_total.cost,  2)                       AS split_cost_usd,
  ROUND(ABS(baseline.cost  - split_total.cost),  2) AS cost_diff_usd,
  CASE
    WHEN ABS(baseline.cost - split_total.cost) < 0.01 THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM baseline, split_total;


-- =============================================================
-- (Reference) Check uncovered cluster_source
-- =============================================================
-- If values other than 'UI', 'API', 'JOB' exist, there may be cost omitted from the split MVs.
-- =============================================================
SELECT
  cluster_source,
  COUNT(DISTINCT job_run_id) AS run_count,
  ROUND(SUM(total_dbus), 4)  AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
WHERE is_serverless = false
GROUP BY cluster_source
ORDER BY total_cost_usd DESC;
