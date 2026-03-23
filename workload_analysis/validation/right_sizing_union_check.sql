-- =====================================================================
-- 검증: right_sizing_analysis_mv UNION 일관성
-- =====================================================================
-- right_sizing_analysis_mv는 all_purpose_cluster_sizing_mv +
-- job_compute_sizing_mv의 UNION ALL이므로,
-- total_dbus, total_cost_usd 합산이 정확히 일치해야 한다.
-- 행 수도 동일해야 한다.
--
-- Parameters:
--   ${source_catalog}   - 파이프라인 target catalog
--   ${analytics_schema} - 파이프라인 target schema
-- =====================================================================


-- =============================================================
-- T7: right_sizing_targets 비용 합산 == ap + jc 비용 합산
-- =============================================================
WITH rst AS (
  SELECT
    COUNT(*)                       AS row_count,
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.right_sizing_analysis_mv
),
ap_jc AS (
  SELECT
    COUNT(*)                       AS row_count,
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
  'T7_right_sizing_union_cost' AS test_name,
  rst.total_dbus       AS rst_dbus,
  ap_jc.total_dbus     AS ap_jc_dbus,
  ROUND(ABS(COALESCE(rst.total_dbus, 0) - COALESCE(ap_jc.total_dbus, 0)), 4) AS dbus_diff,
  rst.total_cost_usd   AS rst_cost,
  ap_jc.total_cost_usd AS ap_jc_cost,
  ROUND(ABS(COALESCE(rst.total_cost_usd, 0) - COALESCE(ap_jc.total_cost_usd, 0)), 2) AS cost_diff,
  CASE
    WHEN ABS(COALESCE(rst.total_dbus, 0)     - COALESCE(ap_jc.total_dbus, 0))     < 0.01
     AND ABS(COALESCE(rst.total_cost_usd, 0) - COALESCE(ap_jc.total_cost_usd, 0)) < 0.01
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM rst, ap_jc;


-- =============================================================
-- T8: right_sizing_targets 행 수 == ap 행 수 + jc 행 수
-- =============================================================
WITH rst_count AS (
  SELECT COUNT(*) AS cnt FROM ${source_catalog}.${analytics_schema}.right_sizing_analysis_mv
),
ap_count AS (
  SELECT COUNT(*) AS cnt FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
),
jc_count AS (
  SELECT COUNT(*) AS cnt FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
)
SELECT
  'T8_right_sizing_union_rowcount' AS test_name,
  rst.cnt               AS rst_rows,
  ap.cnt + jc.cnt       AS ap_jc_rows,
  ap.cnt                AS ap_rows,
  jc.cnt                AS jc_rows,
  CASE
    WHEN rst.cnt = ap.cnt + jc.cnt THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM rst_count rst, ap_count ap, jc_count jc;


-- =============================================================
-- T9: right_sizing_targets compute_type별 비용 == 소스 MV 비용
-- =============================================================
-- compute_type='All-Purpose' 행의 합 == all_purpose_cluster_sizing_mv 합
-- compute_type='Job Compute' 행의 합 == job_compute_sizing_mv 합
-- =============================================================
WITH rst_by_type AS (
  SELECT
    compute_type,
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.right_sizing_analysis_mv
  GROUP BY compute_type
),
ap_mv AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
),
jc_mv AS (
  SELECT
    ROUND(SUM(total_dbus), 4)     AS total_dbus,
    ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
  FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
)
SELECT
  'T9_right_sizing_by_compute_type' AS test_name,
  CASE
    WHEN ABS(COALESCE(rst_ap.total_dbus, 0)     - COALESCE(ap.total_dbus, 0))     < 0.01
     AND ABS(COALESCE(rst_ap.total_cost_usd, 0) - COALESCE(ap.total_cost_usd, 0)) < 0.01
     AND ABS(COALESCE(rst_jc.total_dbus, 0)     - COALESCE(jc.total_dbus, 0))     < 0.01
     AND ABS(COALESCE(rst_jc.total_cost_usd, 0) - COALESCE(jc.total_cost_usd, 0)) < 0.01
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  rst_ap.total_dbus AS rst_ap_dbus, ap.total_dbus AS mv_ap_dbus,
  rst_jc.total_dbus AS rst_jc_dbus, jc.total_dbus AS mv_jc_dbus
FROM ap_mv ap, jc_mv jc,
  (SELECT * FROM rst_by_type WHERE compute_type = 'All-Purpose')  rst_ap,
  (SELECT * FROM rst_by_type WHERE compute_type = 'Job Compute') rst_jc;
