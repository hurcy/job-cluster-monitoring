-- =====================================================================
-- 검증: DBU/비용 롤업 일관성
-- =====================================================================
-- job_run_cost_profiles_mv (is_serverless=false)의
-- total_dbus, total_cost_usd 합산이
-- all_purpose_cluster_sizing_mv + job_compute_sizing_mv 합산과 일치하는지 확인.
--
-- 추가로 cluster_source 기준 분리가 누락 없이 이루어졌는지 검증한다.
--
-- Parameters:
--   ${source_catalog}   - 파이프라인 target catalog
--   ${analytics_schema} - 파이프라인 target schema
-- =====================================================================


-- =============================================================
-- T1: job_run (classic) 합산 == all_purpose + job_compute 합산
-- =============================================================
-- job_run_cost_profiles_mv에서 is_serverless=false인 행의
-- total_dbus, total_cost_usd 합을 기준으로,
-- all_purpose (UI/API) + job_compute (JOB) 합이 동일해야 한다.
--
-- 허용 오차: ROUND 누적 오차를 감안하여 dbus < 1.0, cost < 1.0
-- =============================================================
WITH job_run_classic AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
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
-- T2: cluster_source 분포 확인
-- =============================================================
-- job_run_cost_profiles_mv (is_serverless=false)에서
-- cluster_source가 'UI', 'API', 'JOB' 외 값이 있으면
-- sizing MV에서 누락될 수 있다.
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
FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
WHERE is_serverless = false
GROUP BY cluster_source
ORDER BY total_cost_usd DESC;


-- =============================================================
-- T3: all_purpose 소스 필터 정확성
-- =============================================================
-- all_purpose_cluster_sizing_mv의 total_dbus가
-- job_run에서 cluster_source IN ('UI','API')인 행의 합과 일치하는지 확인.
-- =============================================================
WITH job_run_ap AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
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
-- T4: job_compute 소스 필터 정확성
-- =============================================================
-- job_compute_sizing_mv의 total_dbus가
-- job_run에서 cluster_source = 'JOB'인 행의 합과 일치하는지 확인.
-- =============================================================
WITH job_run_jc AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
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
