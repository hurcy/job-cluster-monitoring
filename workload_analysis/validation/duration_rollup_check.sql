-- =====================================================================
-- 검증: Duration (실행 시간) 롤업 일관성
-- =====================================================================
-- job_run_cost_analysis_mv의 run_duration_minutes 합산이
-- sizing MV의 avg_run_duration_minutes * run_count 합산과
-- 근사적으로 일치하는지 확인.
--
-- 주의: sizing MV는 ROUND(AVG(...), 2)로 집계하므로
-- 역산 시 ROUND 오차가 누적된다. 허용 오차를 넓게 설정한다.
--
-- Parameters:
--   ${source_catalog}   - 파이프라인 target catalog
--   ${analytics_schema} - 파이프라인 target schema
-- =====================================================================


-- =============================================================
-- T10: All-Purpose duration 일관성
-- =============================================================
-- job_run에서 cluster_source IN ('UI','API')인 행의
-- SUM(run_duration_minutes) ≈
-- all_purpose에서 SUM(avg_run_duration_minutes * run_count)
--
-- 허용 오차: 소스 합산의 5% 또는 10분 중 큰 값
-- (ROUND(AVG, 2) 역산 누적 오차 감안)
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
-- T11: Job Compute duration 일관성
-- =============================================================
-- job_run에서 cluster_source = 'JOB'인 행의
-- SUM(run_duration_minutes) ≈
-- job_compute에서 SUM(avg_run_duration_minutes * run_count)
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
-- T12: right_sizing_targets duration 일관성
-- =============================================================
-- right_sizing_targets_mv의 SUM(avg_run_duration_minutes * run_count)가
-- all_purpose + job_compute 합산과 동일해야 한다.
-- (UNION ALL이므로 정확히 일치해야 한다)
-- =============================================================
WITH rst AS (
  SELECT
    ROUND(SUM(avg_run_duration_minutes * run_count), 2) AS total_duration
  FROM ${source_catalog}.${analytics_schema}.right_sizing_targets_mv
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
