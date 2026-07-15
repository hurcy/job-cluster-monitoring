-- =====================================================================
-- Validation: DBU/cost rollup consistency
-- =====================================================================
-- Verify that the sum of total_dbus, total_cost_usd for
-- job_run_cost_analysis_mv (is_serverless=false) matches the sum of
-- all_purpose_cluster_sizing_mv + job_compute_sizing_mv.
--
-- Additionally, verify that the split by cluster_source is complete with no omissions.
--
-- Parameters:
--   ${source_catalog}   - pipeline target catalog
--   ${analytics_schema} - pipeline target schema
-- =====================================================================


-- =============================================================
-- T1: job_run (classic) sum == all_purpose + job_compute sum
-- =============================================================
-- Using the sum of total_dbus, total_cost_usd for rows with
-- is_serverless=false in job_run_cost_analysis_mv as the baseline,
-- the sum of all_purpose (UI/API) + job_compute (JOB) must be equal.
--
-- Tolerance: accounting for accumulated ROUND error, dbus < 1.0, cost < 1.0
-- =============================================================
WITH job_run_classic AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
  WHERE is_serverless = false
),
ap_jc_combined AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM (
    SELECT total_dbus, total_cost_usd
    FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
    UNION ALL
    SELECT total_dbus, total_cost_usd
    FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
  )
)
SELECT
  'T1_dbu_cost_classic_split'                                AS test_name,
  jrc.total_dbus                                             AS job_run_dbus,
  ac.total_dbus                                              AS sizing_dbus,
  ROUND(ABS(COALESCE(jrc.total_dbus, 0)  - COALESCE(ac.total_dbus, 0)), 4)   AS dbus_diff,
  jrc.total_cost_usd                                         AS job_run_cost,
  ac.total_cost_usd                                          AS sizing_cost,
  ROUND(ABS(COALESCE(jrc.total_cost_usd, 0) - COALESCE(ac.total_cost_usd, 0)), 2) AS cost_diff,
  CASE
    WHEN ABS(COALESCE(jrc.total_dbus, 0)     - COALESCE(ac.total_dbus, 0))     < 1.0
     AND ABS(COALESCE(jrc.total_cost_usd, 0) - COALESCE(ac.total_cost_usd, 0)) < 1.0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM job_run_classic jrc, ap_jc_combined ac;


-- =============================================================
-- T2: check cluster_source distribution
-- =============================================================
-- In job_run_cost_analysis_mv (is_serverless=false),
-- if cluster_source has values other than 'UI', 'API', 'JOB',
-- they may be omitted from the sizing MV.
-- =============================================================
SELECT
  'T2_uncovered_cluster_source'  AS test_name,
  cluster_source,
  COUNT(*)                       AS row_count,
  ROUND(SUM(total_dbus), 4)     AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd,
  CASE
    WHEN cluster_source IN ('UI', 'API', 'JOB') THEN 'COVERED'
    ELSE 'UNCOVERED - POTENTIAL COST LEAKAGE'
  END AS coverage_status
FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
WHERE is_serverless = false
GROUP BY cluster_source
ORDER BY total_cost_usd DESC;


-- =============================================================
-- T3: all_purpose source filter accuracy
-- =============================================================
-- Verify that total_dbus of all_purpose_cluster_sizing_mv matches
-- the sum of rows with cluster_source IN ('UI','API') in job_run.
-- =============================================================
WITH job_run_ap AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
  WHERE is_serverless = false
    AND cluster_source IN ('UI', 'API')
),
ap_mv AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
)
SELECT
  'T3_all_purpose_filter_accuracy' AS test_name,
  jr.total_dbus     AS job_run_ap_dbus,
  mv.total_dbus     AS mv_ap_dbus,
  ROUND(ABS(COALESCE(jr.total_dbus, 0) - COALESCE(mv.total_dbus, 0)), 4) AS dbus_diff,
  jr.total_cost_usd AS job_run_ap_cost,
  mv.total_cost_usd AS mv_ap_cost,
  ROUND(ABS(COALESCE(jr.total_cost_usd, 0) - COALESCE(mv.total_cost_usd, 0)), 2) AS cost_diff,
  CASE
    WHEN ABS(COALESCE(jr.total_dbus, 0)     - COALESCE(mv.total_dbus, 0))     < 1.0
     AND ABS(COALESCE(jr.total_cost_usd, 0) - COALESCE(mv.total_cost_usd, 0)) < 1.0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM job_run_ap jr, ap_mv mv;


-- =============================================================
-- T4: job_compute source filter accuracy
-- =============================================================
-- Verify that total_dbus of job_compute_sizing_mv matches
-- the sum of rows with cluster_source = 'JOB' in job_run.
-- =============================================================
WITH job_run_jc AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
  WHERE is_serverless = false
    AND cluster_source = 'JOB'
),
jc_mv AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
)
SELECT
  'T4_job_compute_filter_accuracy' AS test_name,
  jr.total_dbus     AS job_run_jc_dbus,
  mv.total_dbus     AS mv_jc_dbus,
  ROUND(ABS(COALESCE(jr.total_dbus, 0) - COALESCE(mv.total_dbus, 0)), 4) AS dbus_diff,
  jr.total_cost_usd AS job_run_jc_cost,
  mv.total_cost_usd AS mv_jc_cost,
  ROUND(ABS(COALESCE(jr.total_cost_usd, 0) - COALESCE(mv.total_cost_usd, 0)), 2) AS cost_diff,
  CASE
    WHEN ABS(COALESCE(jr.total_dbus, 0)     - COALESCE(mv.total_dbus, 0))     < 1.0
     AND ABS(COALESCE(jr.total_cost_usd, 0) - COALESCE(mv.total_cost_usd, 0)) < 1.0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM job_run_jc jr, jc_mv mv;
