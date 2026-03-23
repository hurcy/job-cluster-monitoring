-- =====================================================================
-- Job & Cluster Monitoring - Lakeflow Declarative Pipeline
-- =====================================================================
-- system_table_copies_mv.sql 파이프라인이 system.*.* 테이블에서
-- 워크스페이스별로 필터링한 MV 사본을 ${source_catalog}.${source_schema}에
-- 생성한다. 이 파이프라인은 해당 사본을 소스로 사용하여 분석 MV를 생성한다.
--
-- 중간 단계(1~8)는 TEMPORARY LIVE VIEW로 처리하고,
-- 최종 출력(9, 10, 11)만 MATERIALIZED VIEW로 영속화한다.
--
-- Source Tables (${source_catalog}.${source_schema}):
--   node_timeline, clusters, job_task_run_timeline, job_run_timeline, jobs, usage, list_prices
--
-- Pipeline DAG:
--
--   ${source_catalog}.${source_schema}.node_timeline
--     ├─ 1. node_count_per_minute_v        [TEMP VIEW]
--     │    └─ 2. node_count_stats_v        [TEMP VIEW]
--     └─ 3. instance_utilization_v         [TEMP VIEW]
--
--   ${source_catalog}.${source_schema}.clusters
--     └─ 4. cluster_config_latest_v        [TEMP VIEW]
--
--   ${source_catalog}.${source_schema}.job_task_run_timeline
--     └─ 5. exploded_task_runs_v           [TEMP VIEW]
--          ├─ 6. task_run_stats_v          [TEMP VIEW]
--          └─ 6b. job_run_cluster_map_v    [TEMP VIEW]
--
--   ${source_catalog}.${source_schema}.jobs
--     └─ 7. job_config_latest_v            [TEMP VIEW]
--
--   ${source_catalog}.${source_schema}.usage + list_prices
--     └─ 8. cluster_job_run_cost_v         [TEMP VIEW]
--
--   ${source_catalog}.${source_schema}.job_run_timeline
--     └─ 8b. job_run_timeline_v            [TEMP VIEW]
--
--   2,3,4,6b,7  ──►  9. instance_workload_analysis_mv         [MV] 최종 출력
--   8,4,7,6,8b,3 ──► 10. job_run_cost_analysis_mv             [MV] 최종 출력
--   10         ──► 12. all_purpose_cluster_sizing_mv         [MV] 최종 출력 (All-Purpose, per-cluster)
--   10         ──► 13. job_compute_sizing_mv                 [MV] 최종 출력 (Job Compute, per-job)
--   12,13      ──► 11. right_sizing_analysis_mv               [MV] 하위호환 UNION (대시보드용)
--
-- Pipeline Configuration Parameters:
--   workspace_id      - 분석 대상 워크스페이스 ID (STRING)
--   source_catalog    - 시스템 테이블 사본 카탈로그 (STRING, default: hurcy)
--   source_schema     - 시스템 테이블 사본 스키마 (STRING, default: default)
--   (날짜 범위: Rolling 90-day window — CURRENT_DATE()-90 ~ CURRENT_DATE())
-- =====================================================================


-- =================================================================
-- 1. node_count_per_minute_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 분 단위 워커 노드 수 집계 (오토스케일 분석용).
-- =================================================================

CREATE TEMPORARY LIVE VIEW node_count_per_minute_v
AS
SELECT
  cluster_id,
  node_type,
  DATE_TRUNC('minute', start_time) AS minute_ts,
  COUNT(DISTINCT instance_id)      AS node_count
FROM ${source_catalog}.${source_schema}.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND start_time <  CURRENT_DATE()
GROUP BY cluster_id, node_type, DATE_TRUNC('minute', start_time);


-- =================================================================
-- 2. node_count_stats_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 클러스터별 노드 수 통계 (평균/최대/표준편차).
-- =================================================================

CREATE TEMPORARY LIVE VIEW node_count_stats_v
AS
SELECT
  cluster_id,
  node_type,
  ROUND(AVG(node_count), 2)    AS avg_node_count,
  ROUND(MAX(node_count), 2)    AS max_node_count,
  ROUND(STDDEV(node_count), 2) AS stddev_node_count
FROM LIVE.node_count_per_minute_v
GROUP BY cluster_id, node_type;


-- =================================================================
-- 3. instance_utilization_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 인스턴스별 리소스 활용률 + 워크로드 프로파일 분류.
-- median, stddev 포함 (sizing 판정용).
-- =================================================================

CREATE TEMPORARY LIVE VIEW instance_utilization_v
AS
SELECT
  workspace_id,
  cluster_id,
  driver,
  node_type,
  instance_id,
  MIN(start_time) AS instance_start_time,
  MAX(end_time)   AS instance_end_time,
  ROUND(AVG(cpu_user_percent + cpu_system_percent), 2)                    AS avg_cpu_util,
  ROUND(PERCENTILE_APPROX(cpu_user_percent + cpu_system_percent, 0.5), 2) AS median_cpu_util,
  ROUND(MAX(cpu_user_percent + cpu_system_percent), 2)                    AS max_cpu_util,
  ROUND(STDDEV(cpu_user_percent + cpu_system_percent), 2)                 AS stddev_cpu_util,
  ROUND(AVG(cpu_wait_percent), 2)                      AS avg_cpu_wait,
  ROUND(MAX(cpu_wait_percent), 2)                      AS max_cpu_wait,
  ROUND(AVG(mem_used_percent), 2)                      AS avg_mem_util,
  ROUND(PERCENTILE_APPROX(mem_used_percent, 0.5), 2)   AS median_mem_util,
  ROUND(MAX(mem_used_percent), 2)                      AS max_mem_util,
  ROUND(STDDEV(mem_used_percent), 2)                   AS stddev_mem_util,
  ROUND(AVG(network_received_bytes) / POW(1024, 2), 2) AS avg_net_mb_rec_minute,
  ROUND(AVG(network_sent_bytes) / POW(1024, 2), 2)     AS avg_net_mb_sent_minute,
  CASE
    WHEN AVG(cpu_user_percent + cpu_system_percent) > 60
     AND AVG(mem_used_percent) < 50
      THEN 'CPU Intensive'
    WHEN AVG(mem_used_percent) > 60
     AND AVG(cpu_user_percent + cpu_system_percent) < 50
      THEN 'Memory Intensive'
    WHEN AVG(cpu_user_percent + cpu_system_percent) > 60
     AND AVG(mem_used_percent) > 60
      THEN 'Balanced - High Utilization'
    WHEN AVG(cpu_user_percent + cpu_system_percent) < 30
     AND AVG(mem_used_percent) < 40
      THEN 'Under-utilized'
    ELSE 'Balanced - Moderate Utilization'
  END AS workload_profile
FROM ${source_catalog}.${source_schema}.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND start_time <  CURRENT_DATE()
GROUP BY workspace_id, cluster_id, driver, node_type, instance_id;


-- =================================================================
-- 4. cluster_config_latest_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 클러스터 구성 정보 (SCD2 최신 버전).
-- =================================================================

CREATE TEMPORARY LIVE VIEW cluster_config_latest_v
AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, cluster_id
      ORDER BY change_time DESC
    ) AS _rn
  FROM ${source_catalog}.${source_schema}.clusters
  WHERE workspace_id = '${workspace_id}'
)
WHERE _rn = 1;


-- =================================================================
-- 5. exploded_task_runs_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 태스크 실행 → 클러스터 매핑.
-- compute_ids 배열을 EXPLODE하여 태스크-클러스터 관계를 1:N으로 풀어낸다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW exploded_task_runs_v
AS
SELECT
  tr.workspace_id,
  tr.job_id,
  tr.run_id,
  tr.job_run_id,
  tr.task_key,
  EXPLODE(tr.compute_ids) AS cluster_id,
  tr.period_start_time,
  tr.period_end_time,
  tr.result_state,
  tr.termination_code
FROM ${source_catalog}.${source_schema}.job_task_run_timeline tr
WHERE tr.workspace_id = '${workspace_id}'
  AND ARRAY_SIZE(tr.compute_ids) > 0
  AND tr.period_start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND tr.period_start_time <  CURRENT_DATE();


-- =================================================================
-- 6. task_run_stats_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- Job Run 단위 집계 (클러스터별 × 잡 실행별).
-- 성공한 태스크가 하나라도 있는 잡 실행만 필터링한다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW task_run_stats_v
AS
SELECT
  workspace_id,
  cluster_id,
  job_id,
  job_run_id,
  COUNT(DISTINCT task_key)          AS task_count,
  MIN(period_start_time)            AS period_start_time,
  MAX(period_end_time)              AS period_end_time,
  COLLECT_SET(result_state)         AS result_states,
  COLLECT_SET(termination_code)     AS termination_codes
FROM LIVE.exploded_task_runs_v
GROUP BY workspace_id, cluster_id, job_id, job_run_id
HAVING ARRAY_CONTAINS(COLLECT_SET(result_state), 'SUCCESS')
    OR ARRAY_CONTAINS(COLLECT_SET(termination_code), 'SUCCESS');


-- =================================================================
-- 6b. job_run_cluster_map_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- Job의 Task가 실행된 cluster_id 매핑 (시간 범위 포함).
-- =================================================================

CREATE TEMPORARY LIVE VIEW job_run_cluster_map_v
AS
SELECT
  workspace_id,
  cluster_id,
  job_id,
  job_run_id,
  MIN(period_start_time)            AS period_start_time,
  MAX(period_end_time)              AS period_end_time
FROM LIVE.exploded_task_runs_v
GROUP BY workspace_id, cluster_id, job_id, job_run_id;


-- =================================================================
-- 7. job_config_latest_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 잡 메타데이터 (SCD2 최신 버전, 삭제되지 않은 잡만).
-- =================================================================

CREATE TEMPORARY LIVE VIEW job_config_latest_v
AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, job_id
      ORDER BY change_time DESC
    ) AS _rn
  FROM ${source_catalog}.${source_schema}.jobs
  WHERE workspace_id = '${workspace_id}'
    AND delete_time IS NULL
)
WHERE _rn = 1;


-- =================================================================
-- 8a. ap_cluster_cost_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- All-Purpose 클러스터 단위 비용 집계.
-- billing_origin_product = 'ALL_PURPOSE'는 job_id가 NULL이므로
-- cluster_id 단위로만 집계한다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW ap_cluster_cost_v
AS
SELECT
  u.workspace_id,
  u.usage_metadata.cluster_id              AS cluster_id,
  ROUND(SUM(u.usage_quantity), 4)          AS total_dbus,
  ROUND(SUM(
    u.usage_quantity
    * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)
  ), 2)                                    AS total_cost_usd
FROM ${source_catalog}.${source_schema}.usage u
LEFT JOIN ${source_catalog}.${source_schema}.list_prices lp
  ON  u.sku_name         = lp.sku_name
  AND u.cloud            = lp.cloud
  AND u.usage_start_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE u.workspace_id = '${workspace_id}'
  AND u.billing_origin_product = 'ALL_PURPOSE'
  AND u.usage_metadata.cluster_id IS NOT NULL
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 90)
  AND u.usage_date <  CURRENT_DATE()
GROUP BY u.workspace_id, u.usage_metadata.cluster_id;


-- =================================================================
-- 8b. ap_cluster_run_count_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- All-Purpose 클러스터별 distinct job_run 수.
-- 비용 균등 배분 시 분모로 사용한다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW ap_cluster_run_count_v
AS
SELECT
  workspace_id,
  cluster_id,
  COUNT(DISTINCT CONCAT(job_id, '-', job_run_id)) AS run_count
FROM LIVE.exploded_task_runs_v
GROUP BY workspace_id, cluster_id;


-- =================================================================
-- 8c. cluster_job_run_cost_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 잡 실행 단위 비용 (DBU + USD).
-- is_serverless 컬럼으로 serverless/classic 비용을 구분한다.
--
-- Job Compute / Serverless (billing_origin_product = 'JOBS'):
--   usage_metadata에 job_id, job_run_id가 존재하므로 직접 사용.
-- All-Purpose (billing_origin_product = 'ALL_PURPOSE'):
--   usage_metadata에 job_id가 NULL이므로, cluster_id 단위 비용을
--   exploded_task_runs_v를 통해 job_id/job_run_id로 매핑한다.
--   비용은 해당 클러스터에서 실행된 job_run 수 비례로 균등 배분한다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW cluster_job_run_cost_v
AS
-- Part 1: Job Compute / Serverless — usage_metadata에 job_id 존재
SELECT
  u.workspace_id,
  u.usage_metadata.cluster_id                AS cluster_id,
  u.usage_metadata.job_id                    AS job_id,
  u.usage_metadata.job_run_id                AS job_run_id,
  COALESCE(u.product_features.is_serverless, false) AS is_serverless,
  ROUND(SUM(u.usage_quantity), 4)            AS total_dbus,
  ROUND(SUM(
    u.usage_quantity
    * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)
  ), 2)                                      AS total_cost_usd
FROM ${source_catalog}.${source_schema}.usage u
LEFT JOIN ${source_catalog}.${source_schema}.list_prices lp
  ON  u.sku_name         = lp.sku_name
  AND u.cloud            = lp.cloud
  AND u.usage_start_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE u.workspace_id = '${workspace_id}'
  AND u.billing_origin_product = 'JOBS'
  AND u.usage_metadata.job_id IS NOT NULL
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 90)
  AND u.usage_date <  CURRENT_DATE()
GROUP BY u.workspace_id, u.usage_metadata.cluster_id, u.usage_metadata.job_id,
         u.usage_metadata.job_run_id, COALESCE(u.product_features.is_serverless, false)

UNION ALL

-- Part 2: All-Purpose — cluster_id 단위 비용을 job_run별로 균등 배분
SELECT
  ac.workspace_id,
  ac.cluster_id,
  etr.job_id,
  etr.job_run_id,
  false                                      AS is_serverless,
  ROUND(ac.total_dbus / rc.run_count, 4)     AS total_dbus,
  ROUND(ac.total_cost_usd / rc.run_count, 2) AS total_cost_usd
FROM LIVE.ap_cluster_cost_v ac
JOIN LIVE.ap_cluster_run_count_v rc
  ON  ac.workspace_id = rc.workspace_id
  AND ac.cluster_id   = rc.cluster_id
  AND rc.run_count > 0
JOIN (
  SELECT DISTINCT workspace_id, cluster_id, job_id, job_run_id
  FROM LIVE.exploded_task_runs_v
) etr
  ON  ac.workspace_id = etr.workspace_id
  AND ac.cluster_id   = etr.cluster_id;


-- =================================================================
-- 9. instance_workload_analysis_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- 인스턴스 수준 프로파일링: 활용률 + 클러스터 구성 + 잡 실행.
-- TEMPORARY VIEW들을 조인하여 최종 MV로 영속화한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW instance_workload_analysis_mv
(
  -- Sanity: 필수 키 NOT NULL
  CONSTRAINT dq_workspace_id_not_null EXPECT (workspace_id IS NOT NULL),
  CONSTRAINT dq_cluster_id_not_null EXPECT (cluster_id IS NOT NULL),
  CONSTRAINT dq_instance_id_not_null EXPECT (instance_id IS NOT NULL),

  -- CPU/Mem 범위 (0~100%)
  CONSTRAINT dq_cpu_util_range EXPECT (avg_cpu_util IS NULL OR (avg_cpu_util >= 0 AND avg_cpu_util <= 100)),
  CONSTRAINT dq_mem_util_range EXPECT (avg_mem_util IS NULL OR (avg_mem_util >= 0 AND avg_mem_util <= 100)),

  -- Workload profile 유효값
  CONSTRAINT dq_valid_workload_profile EXPECT (workload_profile IN (
    'CPU Intensive', 'Memory Intensive', 'Balanced - High Utilization',
    'Under-utilized', 'Balanced - Moderate Utilization'
  ))
)
AS
SELECT
  iu.workspace_id,
  iu.cluster_id,
  cc.cluster_name,
  cc.cluster_source,
  cc.owned_by,
  iu.instance_id,
  iu.driver,
  iu.node_type,
  cc.driver_node_type,
  cc.worker_node_type,
  cc.worker_count          AS configured_workers,
  cc.min_autoscale_workers,
  cc.max_autoscale_workers,
  cc.dbr_version,
  cc.policy_id,
  cc.tags,

  iu.instance_start_time,
  iu.instance_end_time,

  ncs.avg_node_count,
  ncs.max_node_count,
  ncs.stddev_node_count,

  iu.avg_cpu_util,
  iu.median_cpu_util,
  iu.max_cpu_util,
  iu.stddev_cpu_util,
  iu.avg_cpu_wait,
  iu.max_cpu_wait,
  iu.avg_mem_util,
  iu.median_mem_util,
  iu.max_mem_util,
  iu.stddev_mem_util,
  iu.avg_net_mb_rec_minute,
  iu.avg_net_mb_sent_minute,
  iu.workload_profile,

  jrc.job_id,
  j.name                   AS job_name,
  j.run_as,
  j.creator_id,
  jrc.job_run_id,
  jrc.period_start_time                                        AS run_start_time,
  jrc.period_start_time,
  jrc.period_end_time,
  DATEDIFF(MINUTE, jrc.period_start_time, jrc.period_end_time) AS job_run_duration_minutes

FROM LIVE.instance_utilization_v iu

LEFT JOIN LIVE.node_count_stats_v ncs
  ON  iu.cluster_id = ncs.cluster_id
  AND iu.node_type  = ncs.node_type

LEFT JOIN LIVE.cluster_config_latest_v cc
  ON  iu.cluster_id   = cc.cluster_id
  AND iu.workspace_id = cc.workspace_id

LEFT JOIN LIVE.job_run_cluster_map_v jrc
  ON  iu.cluster_id          = jrc.cluster_id
  AND iu.workspace_id        = jrc.workspace_id
  AND iu.instance_start_time <  jrc.period_end_time
  AND jrc.period_start_time  <  iu.instance_end_time

LEFT JOIN LIVE.job_config_latest_v j
  ON  jrc.job_id       = j.job_id
  AND jrc.workspace_id = j.workspace_id
;


-- =================================================================
-- 8b. job_run_timeline_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- Job Run 단위 시간 집계 (job_run_timeline 기반).
-- Serverless job은 cluster_id/compute_ids가 없어 task_run_stats_v에
-- 포함되지 않으므로, job_run_timeline에서 시간 정보를 보완한다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW job_run_timeline_v
AS
SELECT
  workspace_id,
  job_id,
  run_id                               AS job_run_id,
  MIN(period_start_time)               AS period_start_time,
  MAX(period_end_time)                 AS period_end_time
FROM ${source_catalog}.${source_schema}.job_run_timeline
WHERE workspace_id = '${workspace_id}'
  AND period_start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND period_start_time <  CURRENT_DATE()
GROUP BY workspace_id, job_id, run_id;


-- =================================================================
-- 10. job_run_cost_analysis_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- Job Run 단위 비용 프로파일.
-- cluster_job_run_cost_v를 driving table로 1:1 JOIN만 수행.
-- instance_utilization은 downstream sizing MV에서 직접 집계한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW job_run_cost_analysis_mv
(
  -- Sanity: 필수 키 NOT NULL
  CONSTRAINT dq_workspace_id_not_null EXPECT (workspace_id IS NOT NULL),
  CONSTRAINT dq_job_id_not_null EXPECT (job_id IS NOT NULL),
  CONSTRAINT dq_job_run_id_not_null EXPECT (job_run_id IS NOT NULL),

  -- DBU/Cost 집계 정합성
  CONSTRAINT dq_total_dbus_non_negative EXPECT (total_dbus >= 0),
  CONSTRAINT dq_total_cost_non_negative EXPECT (total_cost_usd >= 0),
  CONSTRAINT dq_cost_requires_dbus EXPECT (total_cost_usd = 0 OR total_dbus > 0),

  -- Duration 정합성
  CONSTRAINT dq_duration_non_negative EXPECT (run_duration_minutes IS NULL OR run_duration_minutes >= 0),
  CONSTRAINT dq_run_start_time_not_null EXPECT (run_start_time IS NOT NULL),

  -- Serverless/Classic 구분
  CONSTRAINT dq_classic_has_cluster EXPECT (is_serverless = true OR cluster_id IS NOT NULL)
)
AS
SELECT
  cjc.workspace_id,
  cjc.cluster_id,
  cjc.is_serverless,
  cc.cluster_name,
  cc.cluster_source,
  cc.owned_by,
  cc.worker_node_type,
  cc.worker_count          AS configured_workers,
  cc.min_autoscale_workers,
  cc.max_autoscale_workers,
  cc.dbr_version,
  cc.policy_id,

  cjc.job_id,
  j.name                   AS job_name,
  j.run_as,
  j.creator_id,
  cjc.job_run_id,

  COALESCE(tr.period_start_time, jrt.period_start_time)                   AS run_start_time,
  COALESCE(tr.period_end_time, jrt.period_end_time)                      AS run_end_time,
  DATEDIFF(MINUTE,
    COALESCE(tr.period_start_time, jrt.period_start_time),
    COALESCE(tr.period_end_time, jrt.period_end_time))                   AS run_duration_minutes,
  tr.task_count,

  cjc.total_dbus,
  cjc.total_cost_usd

FROM LIVE.cluster_job_run_cost_v cjc

LEFT JOIN LIVE.cluster_config_latest_v cc
  ON  cjc.cluster_id   = cc.cluster_id
  AND cjc.workspace_id = cc.workspace_id

LEFT JOIN LIVE.job_config_latest_v j
  ON  cjc.job_id       = j.job_id
  AND cjc.workspace_id = j.workspace_id

LEFT JOIN LIVE.task_run_stats_v tr
  ON  cjc.cluster_id   = tr.cluster_id
  AND cjc.workspace_id = tr.workspace_id
  AND cjc.job_id       = tr.job_id
  AND cjc.job_run_id   = tr.job_run_id

LEFT JOIN LIVE.job_run_timeline_v jrt
  ON  cjc.job_id       = jrt.job_id
  AND cjc.job_run_id   = jrt.job_run_id
  AND cjc.workspace_id = jrt.workspace_id
;


-- =================================================================
-- 12. all_purpose_cluster_sizing_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- All-Purpose 클러스터 단위 Right Sizing 대상 식별.
-- All-Purpose 클러스터는 여러 잡이 동일 클러스터를 공유하므로,
-- per-job recommendation은 오해를 유발한다.
-- cluster_id 기준으로 집계하여 클러스터 전체 관점에서 권고를 생성한다.
-- job_run_cost_analysis_mv에서 cluster_source IN ('UI', 'API') 행만 추출.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW all_purpose_cluster_sizing_mv
(
  -- Sanity: All-Purpose MV에 데이터가 존재해야 함
  CONSTRAINT dq_cluster_id_not_null EXPECT (cluster_id IS NOT NULL),
  CONSTRAINT dq_cluster_source_is_ap EXPECT (cluster_source IN ('UI', 'API')),

  -- DBU/Cost 정합성
  CONSTRAINT dq_total_dbus_non_negative EXPECT (total_dbus >= 0),
  CONSTRAINT dq_total_cost_non_negative EXPECT (total_cost_usd >= 0),
  CONSTRAINT dq_cost_requires_dbus EXPECT (total_cost_usd = 0 OR total_dbus > 0),

  -- Duration 정합성
  CONSTRAINT dq_avg_duration_non_negative EXPECT (avg_run_duration_minutes IS NULL OR avg_run_duration_minutes >= 0),

  -- Sizing recommendation 유효값
  CONSTRAINT dq_valid_sizing EXPECT (sizing_recommendation IN (
    'BURST_PATTERN', 'DEFINITE_DOWNSIZE', 'LIKELY_DOWNSIZE',
    'CONSIDER_UPSIZE', 'IO_BOTTLENECK', 'APPROPRIATE'
  ))
)
AS
WITH cluster_util AS (
  -- cluster 단위 utilization 집계 (instance_utilization_v에서 직접)
  SELECT
    workspace_id, cluster_id,
    COUNT(DISTINCT instance_id)                     AS instance_count,
    ROUND(AVG(avg_cpu_util), 2)                     AS avg_cpu_util,
    ROUND(AVG(median_cpu_util), 2)                  AS median_cpu_util,
    ROUND(MAX(max_cpu_util), 2)                     AS peak_cpu_util,
    ROUND(AVG(stddev_cpu_util), 2)                  AS avg_stddev_cpu,
    ROUND(AVG(avg_cpu_wait), 2)                     AS avg_cpu_wait,
    ROUND(AVG(avg_mem_util), 2)                     AS avg_mem_util,
    ROUND(AVG(median_mem_util), 2)                  AS median_mem_util,
    ROUND(MAX(max_mem_util), 2)                     AS peak_mem_util,
    ROUND(AVG(stddev_mem_util), 2)                  AS avg_stddev_mem
  FROM LIVE.instance_utilization_v
  GROUP BY workspace_id, cluster_id
)
SELECT
  cjc.workspace_id,
  cjc.cluster_id,
  cjc.cluster_name,
  cjc.cluster_source,
  cjc.owned_by,
  cjc.worker_node_type,
  cjc.configured_workers,
  cjc.min_autoscale_workers,
  cjc.max_autoscale_workers,
  cjc.dbr_version,
  cjc.policy_id,
  CAST(cjc.run_start_time AS DATE)                            AS run_start_time,

  COUNT(DISTINCT cjc.job_id)                                  AS job_count,
  COUNT(DISTINCT cjc.job_run_id)                              AS run_count,
  ROUND(AVG(cjc.run_duration_minutes), 2)                     AS avg_run_duration_minutes,
  ROUND(STDDEV(cjc.run_duration_minutes), 2)                  AS stddev_run_duration_minutes,

  -- utilization: cluster 단위 직접 집계 (1:1 JOIN)
  cu.avg_cpu_util,
  cu.median_cpu_util,
  cu.peak_cpu_util                                            AS p95_cpu_util,
  cu.peak_cpu_util,
  cu.avg_stddev_cpu,
  cu.avg_mem_util,
  cu.median_mem_util,
  cu.peak_mem_util                                            AS p95_mem_util,
  cu.peak_mem_util,
  cu.avg_stddev_mem,
  cu.avg_cpu_wait,
  COALESCE(cu.instance_count, 0)                              AS avg_instance_count,

  ROUND(SUM(cjc.total_dbus), 4)                               AS total_dbus,
  ROUND(SUM(cjc.total_cost_usd), 2)                           AS total_cost_usd,

  CASE
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.median_cpu_util < 30
     AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'BURST_PATTERN'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 20
     AND cu.avg_mem_util < 30
     AND cu.median_cpu_util < 15
     AND cu.avg_stddev_cpu < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND (cu.peak_cpu_util > 85 OR cu.peak_mem_util > 85)
      THEN 'CONSIDER_UPSIZE'
    WHEN cu.avg_cpu_wait > 10
      THEN 'IO_BOTTLENECK'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 20 AND cu.avg_mem_util < 30
     AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND (cu.peak_cpu_util > 85 OR cu.peak_mem_util > 85)
      THEN '리소스 한계 근접. 워커 추가 또는 더 큰 인스턴스 타입 검토 필요.'
    WHEN cu.avg_cpu_wait > 10
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
       AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
        OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
        THEN 0
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND cu.avg_cpu_util < 20 AND cu.avg_mem_util < 30
       AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
        THEN SUM(cjc.total_cost_usd) * 0.5
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
       AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
        THEN SUM(cjc.total_cost_usd) * 0.3
      ELSE 0
    END, 2
  ) AS estimated_savings_usd

FROM job_run_cost_analysis_mv cjc
LEFT JOIN cluster_util cu
  ON  cjc.cluster_id   = cu.cluster_id
  AND cjc.workspace_id = cu.workspace_id
WHERE cjc.is_serverless = false
  AND cjc.cluster_source IN ('UI', 'API')
GROUP BY
  cjc.workspace_id, cjc.cluster_id, cjc.cluster_name, cjc.cluster_source,
  cjc.owned_by, cjc.worker_node_type,
  cjc.configured_workers, cjc.min_autoscale_workers, cjc.max_autoscale_workers,
  cjc.dbr_version, cjc.policy_id,
  CAST(cjc.run_start_time AS DATE),
  cu.avg_cpu_util, cu.median_cpu_util, cu.peak_cpu_util,
  cu.avg_stddev_cpu, cu.avg_cpu_wait,
  cu.avg_mem_util, cu.median_mem_util, cu.peak_mem_util,
  cu.avg_stddev_mem, cu.instance_count
;


-- =================================================================
-- 13. job_compute_sizing_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- Job Compute 단위 Right Sizing 대상 식별.
-- Job Compute는 잡과 클러스터가 1:1 매핑이므로 per-job 권고가 정확하다.
-- job_run_cost_analysis_mv에서 cluster_source = 'JOB' 행만 추출.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW job_compute_sizing_mv
(
  -- Sanity: Job Compute 필수 키
  CONSTRAINT dq_job_id_not_null EXPECT (job_id IS NOT NULL),
  CONSTRAINT dq_cluster_source_is_job EXPECT (cluster_source = 'JOB'),

  -- DBU/Cost 정합성
  CONSTRAINT dq_total_dbus_non_negative EXPECT (total_dbus >= 0),
  CONSTRAINT dq_total_cost_non_negative EXPECT (total_cost_usd >= 0),
  CONSTRAINT dq_cost_requires_dbus EXPECT (total_cost_usd = 0 OR total_dbus > 0),

  -- Duration 정합성
  CONSTRAINT dq_avg_duration_non_negative EXPECT (avg_run_duration_minutes IS NULL OR avg_run_duration_minutes >= 0),

  -- Sizing recommendation 유효값
  CONSTRAINT dq_valid_sizing EXPECT (sizing_recommendation IN (
    'BURST_PATTERN', 'DEFINITE_DOWNSIZE', 'LIKELY_DOWNSIZE',
    'CONSIDER_UPSIZE', 'IO_BOTTLENECK', 'APPROPRIATE'
  ))
)
AS
WITH cluster_util AS (
  SELECT
    workspace_id, cluster_id,
    COUNT(DISTINCT instance_id)                     AS instance_count,
    ROUND(AVG(avg_cpu_util), 2)                     AS avg_cpu_util,
    ROUND(AVG(median_cpu_util), 2)                  AS median_cpu_util,
    ROUND(MAX(max_cpu_util), 2)                     AS peak_cpu_util,
    ROUND(AVG(stddev_cpu_util), 2)                  AS avg_stddev_cpu,
    ROUND(AVG(avg_cpu_wait), 2)                     AS avg_cpu_wait,
    ROUND(AVG(avg_mem_util), 2)                     AS avg_mem_util,
    ROUND(AVG(median_mem_util), 2)                  AS median_mem_util,
    ROUND(MAX(max_mem_util), 2)                     AS peak_mem_util,
    ROUND(AVG(stddev_mem_util), 2)                  AS avg_stddev_mem
  FROM LIVE.instance_utilization_v
  GROUP BY workspace_id, cluster_id
)
SELECT
  cjc.workspace_id,
  cjc.job_id,
  cjc.job_name,
  cjc.cluster_id,
  cjc.cluster_name,
  cjc.cluster_source,
  cjc.owned_by,
  cjc.worker_node_type,
  cjc.configured_workers,
  cjc.min_autoscale_workers,
  cjc.max_autoscale_workers,
  cjc.dbr_version,
  cjc.policy_id,
  CAST(cjc.run_start_time AS DATE)                            AS run_start_time,

  COUNT(DISTINCT cjc.job_run_id)                              AS run_count,
  ROUND(AVG(cjc.run_duration_minutes), 2)                     AS avg_run_duration_minutes,
  ROUND(STDDEV(cjc.run_duration_minutes), 2)                  AS stddev_run_duration_minutes,

  cu.avg_cpu_util,
  cu.median_cpu_util,
  cu.peak_cpu_util                                            AS p95_cpu_util,
  cu.peak_cpu_util,
  cu.avg_stddev_cpu,
  cu.avg_mem_util,
  cu.median_mem_util,
  cu.peak_mem_util                                            AS p95_mem_util,
  cu.peak_mem_util,
  cu.avg_stddev_mem,
  cu.avg_cpu_wait,
  COALESCE(cu.instance_count, 0)                              AS avg_instance_count,

  ROUND(SUM(cjc.total_dbus), 4)                               AS total_dbus,
  ROUND(SUM(cjc.total_cost_usd), 2)                           AS total_cost_usd,

  CASE
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.median_cpu_util < 30
     AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'BURST_PATTERN'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 20
     AND cu.avg_mem_util < 30
     AND cu.median_cpu_util < 15
     AND cu.avg_stddev_cpu < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND (cu.peak_cpu_util > 85 OR cu.peak_mem_util > 85)
      THEN 'CONSIDER_UPSIZE'
    WHEN cu.avg_cpu_wait > 10
      THEN 'IO_BOTTLENECK'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 20 AND cu.avg_mem_util < 30
     AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND (cu.peak_cpu_util > 85 OR cu.peak_mem_util > 85)
      THEN '리소스 한계 근접. 워커 추가 또는 더 큰 인스턴스 타입 검토 필요.'
    WHEN cu.avg_cpu_wait > 10
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
       AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
        OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
        THEN 0
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND cu.avg_cpu_util < 20 AND cu.avg_mem_util < 30
       AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
        THEN SUM(cjc.total_cost_usd) * 0.5
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
       AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
        THEN SUM(cjc.total_cost_usd) * 0.3
      ELSE 0
    END, 2
  ) AS estimated_savings_usd

FROM job_run_cost_analysis_mv cjc
LEFT JOIN cluster_util cu
  ON  cjc.cluster_id   = cu.cluster_id
  AND cjc.workspace_id = cu.workspace_id
WHERE cjc.is_serverless = false
  AND cjc.cluster_source = 'JOB'
GROUP BY
  cjc.workspace_id, cjc.job_id, cjc.job_name,
  cjc.cluster_id, cjc.cluster_name, cjc.cluster_source,
  cjc.owned_by, cjc.worker_node_type,
  cjc.configured_workers, cjc.min_autoscale_workers, cjc.max_autoscale_workers,
  cjc.dbr_version, cjc.policy_id,
  CAST(cjc.run_start_time AS DATE),
  cu.avg_cpu_util, cu.median_cpu_util, cu.peak_cpu_util,
  cu.avg_stddev_cpu, cu.avg_cpu_wait,
  cu.avg_mem_util, cu.median_mem_util, cu.peak_mem_util,
  cu.avg_stddev_mem, cu.instance_count
;


-- =================================================================
-- 11. right_sizing_analysis_mv  [MATERIALIZED VIEW] — 하위호환 UNION
-- =================================================================
-- MV 12 (All-Purpose, per-cluster) + MV 13 (Job Compute, per-job)의
-- UNION ALL. 대시보드(6.right_sizing_targets)의 하위호환성을 위해 유지.
-- All-Purpose 행은 job_id/job_name을 NULL로 채운다.
-- Job Compute 행은 job_count를 NULL로 채운다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW right_sizing_analysis_mv
(
  -- Sanity
  CONSTRAINT dq_cluster_id_not_null EXPECT (cluster_id IS NOT NULL),
  CONSTRAINT dq_compute_type_valid EXPECT (compute_type IN ('All-Purpose', 'Job Compute')),

  -- DBU/Cost 정합성
  CONSTRAINT dq_total_dbus_non_negative EXPECT (total_dbus >= 0),
  CONSTRAINT dq_total_cost_non_negative EXPECT (total_cost_usd >= 0),

  -- All-Purpose는 job_id NULL, Job Compute는 job_count NULL
  CONSTRAINT dq_ap_job_id_null EXPECT (compute_type != 'All-Purpose' OR job_id IS NULL),
  CONSTRAINT dq_jc_job_count_null EXPECT (compute_type != 'Job Compute' OR job_count IS NULL)
)
AS
-- All-Purpose: per-cluster, 잡 특정 컬럼은 NULL
SELECT
  workspace_id,
  cluster_id,
  cluster_name,
  cluster_source,
  'All-Purpose'            AS compute_type,
  CAST(NULL AS STRING)     AS job_id,
  CAST(NULL AS STRING)     AS job_name,
  job_count,
  owned_by,
  worker_node_type,
  configured_workers,
  min_autoscale_workers,
  max_autoscale_workers,
  dbr_version,
  policy_id,
  run_start_time,
  run_count,
  avg_run_duration_minutes,
  stddev_run_duration_minutes,
  avg_cpu_util,
  median_cpu_util,
  p95_cpu_util,
  peak_cpu_util,
  avg_stddev_cpu,
  avg_cpu_wait,
  avg_mem_util,
  median_mem_util,
  p95_mem_util,
  peak_mem_util,
  avg_stddev_mem,
  avg_instance_count,
  total_dbus,
  total_cost_usd,
  sizing_recommendation,
  recommendation_detail,
  estimated_savings_usd
FROM LIVE.all_purpose_cluster_sizing_mv

UNION ALL

-- Job Compute: per-job, job_count는 NULL (1:1 매핑이므로 불필요)
SELECT
  workspace_id,
  cluster_id,
  cluster_name,
  cluster_source,
  'Job Compute'            AS compute_type,
  job_id,
  job_name,
  CAST(NULL AS BIGINT)     AS job_count,
  owned_by,
  worker_node_type,
  configured_workers,
  min_autoscale_workers,
  max_autoscale_workers,
  dbr_version,
  policy_id,
  run_start_time,
  run_count,
  avg_run_duration_minutes,
  stddev_run_duration_minutes,
  avg_cpu_util,
  median_cpu_util,
  p95_cpu_util,
  peak_cpu_util,
  avg_stddev_cpu,
  avg_cpu_wait,
  avg_mem_util,
  median_mem_util,
  p95_mem_util,
  peak_mem_util,
  avg_stddev_mem,
  avg_instance_count,
  total_dbus,
  total_cost_usd,
  sizing_recommendation,
  recommendation_detail,
  estimated_savings_usd
FROM LIVE.job_compute_sizing_mv
;
