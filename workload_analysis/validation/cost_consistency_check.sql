-- =====================================================================
-- 비용 일관성 검증: right_sizing_targets_mv 분리 전후 비교
-- =====================================================================
-- all_purpose_cluster_sizing_mv (MV 12) + job_compute_sizing_mv (MV 13)의
-- total_dbus, total_cost_usd 합산이 분리 전 소스와 일치하는지 확인한다.
--
-- 검증 원칙:
--   job_run_cost_profiles_mv (is_serverless=false) 합산
--   == MV12 합산 + MV13 합산
--
-- 실행 방법:
--   파이프라인 배포 후 아래 쿼리를 순서대로 실행하여 result = 'PASS' 확인
--
-- Parameters:
--   ${source_catalog}   - 파이프라인 target catalog (예: hurcy)
--   ${analytics_schema} - 파이프라인 target schema  (예: test)
-- =====================================================================


-- =============================================================
-- 검증 1: 소스 기준값 — job_run_cost_profiles_mv (classic compute)
-- =============================================================
-- 분리 전후 공통 기준값. is_serverless=false 전체 합산.
-- =============================================================
SELECT
  'source_baseline'             AS check_name,
  ROUND(SUM(total_dbus), 4)     AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
WHERE is_serverless = false;


-- =============================================================
-- 검증 2: 분리 후 합산 — MV12 (All-Purpose) + MV13 (Job Compute)
-- =============================================================
-- cluster_source 기준 필터로 분리되었으므로 합산 시 전체를 커버해야 한다.
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
-- 검증 3: 차이 확인 — PASS / FAIL 판정
-- =============================================================
-- cost 차이가 0.01 미만이면 PASS.
-- FAIL 시 cluster_source가 'UI', 'API', 'JOB' 외 값이 있는지 확인할 것.
-- =============================================================
WITH baseline AS (
  SELECT
    SUM(total_dbus)     AS dbus,
    SUM(total_cost_usd) AS cost
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
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
-- (참고) 커버되지 않는 cluster_source 확인
-- =============================================================
-- 'UI', 'API', 'JOB' 외 값이 존재하면 분리 MVs에서 누락된 비용이 있을 수 있다.
-- =============================================================
SELECT
  cluster_source,
  COUNT(DISTINCT job_run_id) AS run_count,
  ROUND(SUM(total_dbus), 4)  AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
WHERE is_serverless = false
GROUP BY cluster_source
ORDER BY total_cost_usd DESC;
