-- =====================================================================
-- 전체 검증 실행: 모든 MV 집계 일관성 테스트
-- =====================================================================
-- 이 파일은 개별 검증 파일의 핵심 테스트를 하나의 쿼리로 통합하여
-- 전체 PASS/FAIL을 한 번에 확인할 수 있도록 한다.
--
-- 실행 결과: test_name | result 형태로 모든 테스트 결과 출력
-- 모든 행이 PASS이면 집계 일관성 검증 통과.
--
-- Parameters:
--   ${source_catalog}   - 파이프라인 target catalog (예: hurcy)
--   ${analytics_schema} - 파이프라인 target schema  (예: test)
-- =====================================================================

WITH
-- ---------------------------------------------------------------
-- 기초 집계
-- ---------------------------------------------------------------
job_run_all AS (
  SELECT
    SUM(total_dbus)                                       AS total_dbus,
    SUM(total_cost_usd)                                   AS total_cost_usd,
    SUM(CASE WHEN is_serverless = false THEN total_dbus ELSE 0 END)     AS classic_dbus,
    SUM(CASE WHEN is_serverless = false THEN total_cost_usd ELSE 0 END) AS classic_cost,
    SUM(CASE WHEN is_serverless = true  THEN total_dbus ELSE 0 END)     AS serverless_dbus,
    SUM(CASE WHEN is_serverless = true  THEN total_cost_usd ELSE 0 END) AS serverless_cost,
    SUM(CASE WHEN is_serverless = false AND cluster_source IN ('UI','API') THEN total_dbus ELSE 0 END)     AS ap_src_dbus,
    SUM(CASE WHEN is_serverless = false AND cluster_source IN ('UI','API') THEN total_cost_usd ELSE 0 END) AS ap_src_cost,
    SUM(CASE WHEN is_serverless = false AND cluster_source = 'JOB'         THEN total_dbus ELSE 0 END)     AS jc_src_dbus,
    SUM(CASE WHEN is_serverless = false AND cluster_source = 'JOB'         THEN total_cost_usd ELSE 0 END) AS jc_src_cost,
    SUM(CASE WHEN is_serverless = false AND cluster_source NOT IN ('UI','API','JOB') AND cluster_source IS NOT NULL THEN total_dbus ELSE 0 END) AS uncovered_dbus,
    SUM(CASE WHEN is_serverless = false AND cluster_source IS NULL THEN total_dbus ELSE 0 END) AS null_source_dbus,
    SUM(CASE WHEN is_serverless = false AND cluster_source IS NULL THEN total_cost_usd ELSE 0 END) AS null_source_cost,
    SUM(CASE WHEN is_serverless = false AND cluster_source IN ('UI','API') THEN run_duration_minutes ELSE 0 END)  AS ap_src_duration,
    SUM(CASE WHEN is_serverless = false AND cluster_source = 'JOB'         THEN run_duration_minutes ELSE 0 END)  AS jc_src_duration,
    SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END)      AS neg_dbus_cnt,
    SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END)  AS neg_cost_cnt,
    SUM(CASE WHEN run_duration_minutes < 0 THEN 1 ELSE 0 END) AS neg_dur_cnt,
    SUM(CASE WHEN is_serverless = false AND cluster_id IS NOT NULL
              AND (workspace_id IS NULL OR job_id IS NULL OR job_run_id IS NULL
                   OR total_dbus IS NULL OR total_cost_usd IS NULL) THEN 1 ELSE 0 END) AS null_key_cnt,
    COUNT(*) AS total_rows
  FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
),
ap_mv AS (
  SELECT
    COALESCE(SUM(total_dbus), 0)     AS total_dbus,
    COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
    COALESCE(SUM(avg_run_duration_minutes * run_count), 0) AS total_duration,
    COALESCE(SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END), 0) AS neg_dbus_cnt,
    COUNT(*) AS row_count
  FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv
),
jc_mv AS (
  SELECT
    COALESCE(SUM(total_dbus), 0)     AS total_dbus,
    COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
    COALESCE(SUM(avg_run_duration_minutes * run_count), 0) AS total_duration,
    COALESCE(SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END), 0) AS neg_dbus_cnt,
    COUNT(*) AS row_count
  FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv
),
rst_mv AS (
  SELECT
    COALESCE(SUM(total_dbus), 0)     AS total_dbus,
    COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
    COALESCE(SUM(avg_run_duration_minutes * run_count), 0) AS total_duration,
    COALESCE(SUM(CASE WHEN compute_type = 'All-Purpose' THEN total_dbus ELSE 0 END), 0) AS ap_dbus,
    COALESCE(SUM(CASE WHEN compute_type = 'Job Compute' THEN total_dbus ELSE 0 END), 0) AS jc_dbus,
    COALESCE(SUM(CASE WHEN compute_type = 'All-Purpose' THEN total_cost_usd ELSE 0 END), 0) AS ap_cost,
    COALESCE(SUM(CASE WHEN compute_type = 'Job Compute' THEN total_cost_usd ELSE 0 END), 0) AS jc_cost,
    COUNT(*) AS row_count
  FROM ${source_catalog}.${analytics_schema}.right_sizing_targets_mv
),
iwp_mv AS (
  SELECT COUNT(*) AS row_count
  FROM ${source_catalog}.${analytics_schema}.instance_workload_profiles_mv
)

-- ---------------------------------------------------------------
-- 테스트 결과
-- ---------------------------------------------------------------
-- T1: attributable classic split (ap_src + jc_src == ap + jc)
-- NULL cluster_source 행은 billing에서 cluster_id가 없어 sizing 대상에서 제외됨
SELECT 'T01_classic_dbu_cost_split' AS test_name,
  CASE WHEN ABS((jr.ap_src_dbus + jr.jc_src_dbus) - (ap.total_dbus + jc.total_dbus)) < 1.0
        AND ABS((jr.ap_src_cost + jr.jc_src_cost) - (ap.total_cost_usd + jc.total_cost_usd)) < 1.0
    THEN 'PASS' ELSE 'FAIL' END AS result,
  CONCAT('attributable=', ROUND(jr.ap_src_dbus + jr.jc_src_dbus,2),
         ' sizing=', ROUND(ap.total_dbus + jc.total_dbus,2),
         ' diff=', ROUND(ABS((jr.ap_src_dbus + jr.jc_src_dbus) - (ap.total_dbus + jc.total_dbus)),4)) AS detail
FROM job_run_all jr, ap_mv ap, jc_mv jc

UNION ALL

-- T2: uncovered cluster_source
SELECT 'T02_no_uncovered_cluster_source',
  CASE WHEN jr.uncovered_dbus < 0.01 THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('uncovered_dbus=', ROUND(jr.uncovered_dbus, 4))
FROM job_run_all jr

UNION ALL

-- T3: all_purpose source filter accuracy
SELECT 'T03_all_purpose_source_filter',
  CASE WHEN ABS(jr.ap_src_dbus - ap.total_dbus) < 1.0
        AND ABS(jr.ap_src_cost - ap.total_cost_usd) < 1.0
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('src=', ROUND(jr.ap_src_dbus,2), ' mv=', ROUND(ap.total_dbus,2),
         ' diff=', ROUND(ABS(jr.ap_src_dbus - ap.total_dbus),4))
FROM job_run_all jr, ap_mv ap

UNION ALL

-- T4: job_compute source filter accuracy
SELECT 'T04_job_compute_source_filter',
  CASE WHEN ABS(jr.jc_src_dbus - jc.total_dbus) < 1.0
        AND ABS(jr.jc_src_cost - jc.total_cost_usd) < 1.0
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('src=', ROUND(jr.jc_src_dbus,2), ' mv=', ROUND(jc.total_dbus,2),
         ' diff=', ROUND(ABS(jr.jc_src_dbus - jc.total_dbus),4))
FROM job_run_all jr, jc_mv jc

UNION ALL

-- T5: serverless + classic = total
SELECT 'T05_serverless_classic_partition',
  CASE WHEN ABS(jr.total_dbus - (jr.classic_dbus + jr.serverless_dbus)) < 0.01
        AND ABS(jr.total_cost_usd - (jr.classic_cost + jr.serverless_cost)) < 0.01
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('total=', ROUND(jr.total_dbus,2),
         ' classic=', ROUND(jr.classic_dbus,2),
         ' serverless=', ROUND(jr.serverless_dbus,2))
FROM job_run_all jr

UNION ALL

-- T6: serverless exclusion (total - sizing - serverless - null_source ≈ 0)
-- null_source: billing에서 cluster_id가 NULL인 classic 행 (sizing 대상 아님)
SELECT 'T06_serverless_excluded_from_sizing',
  CASE WHEN ABS(jr.total_dbus - (ap.total_dbus + jc.total_dbus) - jr.serverless_dbus - jr.null_source_dbus) < 1.0
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('residual=', ROUND(ABS(jr.total_dbus - (ap.total_dbus + jc.total_dbus) - jr.serverless_dbus - jr.null_source_dbus),4),
         ' null_source=', ROUND(jr.null_source_dbus,4))
FROM job_run_all jr, ap_mv ap, jc_mv jc

UNION ALL

-- T7: right_sizing cost == ap + jc cost
SELECT 'T07_right_sizing_union_cost',
  CASE WHEN ABS(rst.total_dbus - (ap.total_dbus + jc.total_dbus)) < 0.01
        AND ABS(rst.total_cost_usd - (ap.total_cost_usd + jc.total_cost_usd)) < 0.01
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('rst=', ROUND(rst.total_dbus,2), ' ap+jc=', ROUND(ap.total_dbus + jc.total_dbus,2))
FROM rst_mv rst, ap_mv ap, jc_mv jc

UNION ALL

-- T8: right_sizing row count == ap rows + jc rows
SELECT 'T08_right_sizing_union_rowcount',
  CASE WHEN rst.row_count = ap.row_count + jc.row_count
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('rst=', rst.row_count, ' ap=', ap.row_count, ' jc=', jc.row_count)
FROM rst_mv rst, ap_mv ap, jc_mv jc

UNION ALL

-- T9: right_sizing by compute_type matches source MVs
SELECT 'T09_right_sizing_by_compute_type',
  CASE WHEN ABS(rst.ap_dbus - ap.total_dbus) < 0.01
        AND ABS(rst.jc_dbus - jc.total_dbus) < 0.01
        AND ABS(rst.ap_cost - ap.total_cost_usd) < 0.01
        AND ABS(rst.jc_cost - jc.total_cost_usd) < 0.01
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('rst_ap=', ROUND(rst.ap_dbus,2), ' mv_ap=', ROUND(ap.total_dbus,2),
         ' rst_jc=', ROUND(rst.jc_dbus,2), ' mv_jc=', ROUND(jc.total_dbus,2))
FROM rst_mv rst, ap_mv ap, jc_mv jc

UNION ALL

-- T10: all_purpose duration consistency
SELECT 'T10_all_purpose_duration',
  CASE
    WHEN jr.ap_src_duration = 0 AND ap.total_duration = 0 THEN 'PASS'
    WHEN ABS(jr.ap_src_duration - ap.total_duration)
         < GREATEST(ABS(jr.ap_src_duration) * 0.05, 10)
      THEN 'PASS'
    ELSE 'FAIL'
  END,
  CONCAT('src=', ROUND(jr.ap_src_duration,2), ' mv=', ROUND(ap.total_duration,2),
         ' diff=', ROUND(ABS(jr.ap_src_duration - ap.total_duration),2))
FROM job_run_all jr, ap_mv ap

UNION ALL

-- T11: job_compute duration consistency
SELECT 'T11_job_compute_duration',
  CASE
    WHEN jr.jc_src_duration = 0 AND jc.total_duration = 0 THEN 'PASS'
    WHEN ABS(jr.jc_src_duration - jc.total_duration)
         < GREATEST(ABS(jr.jc_src_duration) * 0.05, 10)
      THEN 'PASS'
    ELSE 'FAIL'
  END,
  CONCAT('src=', ROUND(jr.jc_src_duration,2), ' mv=', ROUND(jc.total_duration,2),
         ' diff=', ROUND(ABS(jr.jc_src_duration - jc.total_duration),2))
FROM job_run_all jr, jc_mv jc

UNION ALL

-- T12: right_sizing duration == ap + jc duration
SELECT 'T12_right_sizing_duration',
  CASE WHEN ABS(rst.total_duration - (ap.total_duration + jc.total_duration)) < 0.01
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('rst=', ROUND(rst.total_duration,2),
         ' ap+jc=', ROUND(ap.total_duration + jc.total_duration,2))
FROM rst_mv rst, ap_mv ap, jc_mv jc

UNION ALL

-- T13: no negative dbus/cost in job_run
SELECT 'T13_no_negative_cost_job_run',
  CASE WHEN jr.neg_dbus_cnt = 0 AND jr.neg_cost_cnt = 0
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('neg_dbus=', jr.neg_dbus_cnt, ' neg_cost=', jr.neg_cost_cnt)
FROM job_run_all jr

UNION ALL

-- T14: no negative dbus/cost in sizing MVs
SELECT 'T14_no_negative_cost_sizing',
  CASE WHEN ap.neg_dbus_cnt = 0 AND jc.neg_dbus_cnt = 0
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('ap_neg=', ap.neg_dbus_cnt, ' jc_neg=', jc.neg_dbus_cnt)
FROM ap_mv ap, jc_mv jc

UNION ALL

-- T15: no null keys in job_run
SELECT 'T15_no_null_keys_job_run',
  CASE WHEN jr.null_key_cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('null_key_rows=', jr.null_key_cnt)
FROM job_run_all jr

UNION ALL

-- T16: no negative duration
SELECT 'T16_no_negative_duration',
  CASE WHEN jr.neg_dur_cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('neg_duration_rows=', jr.neg_dur_cnt)
FROM job_run_all jr

UNION ALL

-- T17: all MVs non-empty
SELECT 'T17_all_mvs_non_empty',
  CASE WHEN jr.total_rows > 0
        AND rst.row_count > 0
        AND iwp.row_count > 0
    THEN 'PASS' ELSE 'FAIL' END,
  CONCAT('job_run=', jr.total_rows, ' rst=', rst.row_count,
         ' ap=', ap.row_count, ' jc=', jc.row_count, ' iwp=', iwp.row_count)
FROM job_run_all jr, rst_mv rst, ap_mv ap, jc_mv jc, iwp_mv iwp

ORDER BY test_name;
