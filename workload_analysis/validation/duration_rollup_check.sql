-- =====================================================================
-- Validation: Duration (run time) rollup consistency
-- =====================================================================
-- Verify that the sum of run_duration_minutes in job_run_cost_analysis_mv
-- approximately matches the sum of avg_run_duration_minutes * run_count
-- in the sizing MV.
--
-- Note: the sizing MV aggregates with ROUND(AVG(...), 2), so
-- ROUND error accumulates when reversing the calculation. Set a wide tolerance.
--
-- Parameters:
--   ${source_catalog}   - pipeline target catalog
--   ${analytics_schema} - pipeline target schema
-- =====================================================================


-- =============================================================
-- T10: All-Purpose duration consistency
-- =============================================================
-- For rows with cluster_source IN ('UI','API') in job_run,
-- SUM(run_duration_minutes) ≈
-- SUM(avg_run_duration_minutes * run_count) in all_purpose
--
-- Tolerance: the greater of 5% of the source sum or 10 minutes
-- (accounting for accumulated ROUND(AVG, 2) reversal error)
-- =============================================================
WITH job_run_ap AS (
  SELECT
    SUM(run_duration_minutes) AS total_duration_minutes,
    COUNT(*)                  AS row_count
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
  WHERE is_serverless = false
    AND cluster_source IN ('UI', 'API')
),
ap_mv AS (
  SELECT
    SUM(avg_run_duration_minutes * run_count) AS total_duration_minutes,
    SUM(run_count)                            AS total_runs
  FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
)
SELECT
  'T10_all_purpose_duration' AS test_name,
  ROUND(jr.total_duration_minutes, 2) AS job_run_duration,
  ROUND(mv.total_duration_minutes, 2) AS sizing_duration,
  ROUND(ABS(COALESCE(jr.total_duration_minutes, 0)
          - COALESCE(mv.total_duration_minutes, 0)), 2) AS duration_diff,
  jr.row_count AS job_run_rows,
  mv.total_runs AS sizing_runs,
  CASE
    WHEN COALESCE(jr.total_duration_minutes, 0) = 0
     AND COALESCE(mv.total_duration_minutes, 0) = 0
      THEN 'PASS'
    WHEN ABS(COALESCE(jr.total_duration_minutes, 0)
           - COALESCE(mv.total_duration_minutes, 0))
         < GREATEST(ABS(COALESCE(jr.total_duration_minutes, 0)) * 0.05, 10)
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM job_run_ap jr, ap_mv mv;


-- =============================================================
-- T11: Job Compute duration consistency
-- =============================================================
-- For rows with cluster_source = 'JOB' in job_run,
-- SUM(run_duration_minutes) ≈
-- SUM(avg_run_duration_minutes * run_count) in job_compute
-- =============================================================
WITH job_run_jc AS (
  SELECT
    SUM(run_duration_minutes) AS total_duration_minutes,
    COUNT(*)                  AS row_count
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
  WHERE is_serverless = false
    AND cluster_source = 'JOB'
),
jc_mv AS (
  SELECT
    SUM(avg_run_duration_minutes * run_count) AS total_duration_minutes,
    SUM(run_count)                            AS total_runs
  FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
)
SELECT
  'T11_job_compute_duration' AS test_name,
  ROUND(jr.total_duration_minutes, 2) AS job_run_duration,
  ROUND(mv.total_duration_minutes, 2) AS sizing_duration,
  ROUND(ABS(COALESCE(jr.total_duration_minutes, 0)
          - COALESCE(mv.total_duration_minutes, 0)), 2) AS duration_diff,
  jr.row_count AS job_run_rows,
  mv.total_runs AS sizing_runs,
  CASE
    WHEN COALESCE(jr.total_duration_minutes, 0) = 0
     AND COALESCE(mv.total_duration_minutes, 0) = 0
      THEN 'PASS'
    WHEN ABS(COALESCE(jr.total_duration_minutes, 0)
           - COALESCE(mv.total_duration_minutes, 0))
         < GREATEST(ABS(COALESCE(jr.total_duration_minutes, 0)) * 0.05, 10)
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM job_run_jc jr, jc_mv mv;


-- =============================================================
-- T12: right_sizing_targets duration consistency
-- =============================================================
-- SUM(avg_run_duration_minutes * run_count) of right_sizing_analysis_mv
-- must equal the sum of all_purpose + job_compute.
-- (must match exactly since it is a UNION ALL)
-- =============================================================
WITH rst AS (
  SELECT
    ROUND(SUM(avg_run_duration_minutes * run_count), 2) AS total_duration
  FROM ${source_catalog}.${analytics_schema}.right_sizing_analysis_mv
),
ap_jc AS (
  SELECT
    ROUND(SUM(total_duration), 2) AS total_duration
  FROM (
    SELECT SUM(avg_run_duration_minutes * run_count) AS total_duration
    FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
    UNION ALL
    SELECT SUM(avg_run_duration_minutes * run_count) AS total_duration
    FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
  )
)
SELECT
  'T12_right_sizing_duration' AS test_name,
  rst.total_duration   AS rst_duration,
  ap_jc.total_duration AS ap_jc_duration,
  ROUND(ABS(COALESCE(rst.total_duration, 0) - COALESCE(ap_jc.total_duration, 0)), 2) AS duration_diff,
  CASE
    WHEN ABS(COALESCE(rst.total_duration, 0) - COALESCE(ap_jc.total_duration, 0)) < 0.01
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM rst, ap_jc;
