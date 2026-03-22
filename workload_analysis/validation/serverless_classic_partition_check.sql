-- =====================================================================
-- 검증: Serverless / Classic 파티션 일관성
-- =====================================================================
-- job_run_cost_profiles_mv 전체 합산이
-- serverless + classic 분리 합산과 동일한지 확인.
-- sizing MV에 serverless 행이 포함되지 않았는지 검증.
--
-- Parameters:
--   ${source_catalog}   - 파이프라인 target catalog
--   ${analytics_schema} - 파이프라인 target schema
-- =====================================================================


-- =============================================================
-- T5: 전체 = serverless + classic 파티션 일관성
-- =============================================================
WITH total AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
),
by_serverless AS (
  SELECT
    is_serverless,
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
  GROUP BY is_serverless
),
partitioned AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM by_serverless
)
SELECT
  'T5_serverless_classic_partition' AS test_name,
  t.total_dbus          AS all_dbus,
  p.total_dbus          AS partitioned_dbus,
  ROUND(ABS(COALESCE(t.total_dbus, 0) - COALESCE(p.total_dbus, 0)), 4) AS dbus_diff,
  t.total_cost_usd      AS all_cost,
  p.total_cost_usd      AS partitioned_cost,
  ROUND(ABS(COALESCE(t.total_cost_usd, 0) - COALESCE(p.total_cost_usd, 0)), 2) AS cost_diff,
  CASE
    WHEN ABS(COALESCE(t.total_dbus, 0) - COALESCE(p.total_dbus, 0)) < 0.01
     AND ABS(COALESCE(t.total_cost_usd, 0) - COALESCE(p.total_cost_usd, 0)) < 0.01
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM total t, partitioned p;


-- =============================================================
-- T6: Serverless 비용이 sizing MV에 포함되지 않았는지 확인
-- =============================================================
-- all_purpose + job_compute + serverless = job_run 전체
-- 즉, job_run 전체 - (ap + jc) = serverless 비용과 동일해야 한다.
-- =============================================================
WITH job_run_all AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
),
job_run_serverless AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_profiles_mv
  WHERE is_serverless = true
),
sizing_total AS (
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
  'T6_serverless_exclusion'  AS test_name,
  a.total_dbus               AS all_dbus,
  st.total_dbus              AS sizing_dbus,
  s.total_dbus               AS serverless_dbus,
  ROUND(ABS(COALESCE(a.total_dbus, 0)
          - COALESCE(st.total_dbus, 0)
          - COALESCE(s.total_dbus, 0)), 4) AS residual_dbus,
  ROUND(ABS(COALESCE(a.total_cost_usd, 0)
          - COALESCE(st.total_cost_usd, 0)
          - COALESCE(s.total_cost_usd, 0)), 2) AS residual_cost,
  CASE
    WHEN ABS(COALESCE(a.total_dbus, 0)
           - COALESCE(st.total_dbus, 0)
           - COALESCE(s.total_dbus, 0)) < 1.0
     AND ABS(COALESCE(a.total_cost_usd, 0)
           - COALESCE(st.total_cost_usd, 0)
           - COALESCE(s.total_cost_usd, 0)) < 1.0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM job_run_all a, job_run_serverless s, sizing_total st;
