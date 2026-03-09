-- =====================================================================
-- Job & Cluster Monitoring - Lakeflow Declarative Pipeline
-- =====================================================================
-- 각 CTE를 독립된 MV / Streaming Table로 분리하여
-- 파이프라인 UI에서 단계별 데이터를 조회·디버깅할 수 있도록 구성한다.
--
-- Pipeline DAG:
--
--   system.compute.node_timeline
--     ├─ 1. node_count_per_minute_mv
--     │    └─ 2. node_count_stats_mv
--     └─ 3. instance_utilization_mv
--
--   system.compute.clusters
--     └─ 4. cluster_config_latest_mv
--
--   system.lakeflow.job_task_run_timeline
--     └─ 5. exploded_task_runs_st  (Streaming Table)
--          └─ 6. task_run_stats_mv
--
--   system.lakeflow.jobs
--     └─ 7. job_config_latest_mv
--
--   system.billing.usage + list_prices
--     └─ 8. cluster_job_cost_mv
--
--   2,3,4,6,7,8  ──►  9. instance_workload_profiles_mv
--
-- Pipeline Configuration Parameters:
--   workspace_id      - 분석 대상 워크스페이스 ID (STRING)
--   system_catalog    - 시스템 테이블 카탈로그 (STRING, default: system)
--   schema_compute    - compute 스키마 (STRING, default: compute)
--   schema_lakeflow   - lakeflow 스키마 (STRING, default: lakeflow)
--   schema_billing    - billing 스키마 (STRING, default: billing)
--   start_date        - 조회 시작일 (STRING, format: yyyy-MM-dd)
--   end_date          - 조회 종료일 (STRING, format: yyyy-MM-dd)
-- =====================================================================


-- =================================================================
-- 1. node_count_per_minute_mv  [MV]
-- =================================================================
-- 분 단위 워커 노드 수 집계 (오토스케일 분석용).
-- node_timeline에서 분 단위로 클러스터별 활성 인스턴스 수를 센다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW node_count_per_minute_mv
AS
SELECT
  cluster_id,
  node_type,
  DATE_TRUNC('minute', start_time) AS minute_ts,
  COUNT(DISTINCT instance_id)      AS node_count
FROM ${system_catalog}.${schema_compute}.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= '${start_date}'
  AND start_time <  '${end_date}'
GROUP BY cluster_id, node_type, DATE_TRUNC('minute', start_time);


-- =================================================================
-- 2. node_count_stats_mv  [MV]
-- =================================================================
-- 클러스터별 노드 수 통계 (평균/최대/표준편차).
-- 오토스케일 클러스터의 실제 가동 워커 수 분포를 파악한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW node_count_stats_mv
AS
SELECT
  cluster_id,
  node_type,
  ROUND(AVG(node_count), 2)    AS avg_node_count,
  ROUND(MAX(node_count), 2)    AS max_node_count,
  ROUND(STDDEV(node_count), 2) AS stddev_node_count
FROM node_count_per_minute_mv
GROUP BY cluster_id, node_type;


-- =================================================================
-- 3. instance_utilization_mv  [MV]
-- =================================================================
-- 인스턴스별 리소스 활용률 + 워크로드 프로파일 분류.
-- CPU, Memory, Network 메트릭을 인스턴스 단위로 집계하고,
-- 활용 패턴에 따라 5가지 워크로드 프로파일을 부여한다.
-- (워커 노드 대상, 드라이버 제외)
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW instance_utilization_mv
AS
SELECT
  workspace_id,
  cluster_id,
  driver,
  node_type,
  instance_id,
  MIN(start_time) AS instance_start_time,
  MAX(end_time)   AS instance_end_time,
  ROUND(AVG(cpu_user_percent + cpu_system_percent), 2) AS avg_cpu_util,
  ROUND(MAX(cpu_user_percent + cpu_system_percent), 2) AS max_cpu_util,
  ROUND(AVG(cpu_wait_percent), 2)                      AS avg_cpu_wait,
  ROUND(MAX(cpu_wait_percent), 2)                      AS max_cpu_wait,
  ROUND(AVG(mem_used_percent), 2)                      AS avg_mem_util,
  ROUND(MAX(mem_used_percent), 2)                      AS max_mem_util,
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
FROM ${system_catalog}.${schema_compute}.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= '${start_date}'
  AND start_time <  '${end_date}'
GROUP BY workspace_id, cluster_id, driver, node_type, instance_id;


-- =================================================================
-- 4. cluster_config_latest_mv  [MV]
-- =================================================================
-- 클러스터 구성 정보 (SCD2 최신 버전).
-- 클러스터별 가장 최근 구성 스냅샷을 제공한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW cluster_config_latest_mv
AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, cluster_id
      ORDER BY change_time DESC
    ) AS _rn
  FROM ${system_catalog}.${schema_compute}.clusters
  WHERE workspace_id = '${workspace_id}'
)
WHERE _rn = 1;


-- =================================================================
-- 5. exploded_task_runs_st  [Streaming Table]
-- =================================================================
-- 태스크 실행 → 클러스터 매핑.
-- job_task_run_timeline은 append-only이므로 Streaming Table로
-- 증분 처리한다. compute_ids 배열을 EXPLODE하여 태스크-클러스터
-- 관계를 1:N으로 풀어낸다.
-- =================================================================

CREATE OR REFRESH STREAMING TABLE exploded_task_runs_st
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
FROM STREAM(${system_catalog}.${schema_lakeflow}.job_task_run_timeline) tr
WHERE tr.workspace_id = '${workspace_id}'
  AND ARRAY_SIZE(tr.compute_ids) > 0
  AND tr.period_start_time >= '${start_date}'
  AND tr.period_start_time <  '${end_date}';


-- =================================================================
-- 6. task_run_stats_mv  [MV]
-- =================================================================
-- 잡 실행 단위 집계 (클러스터별 × 잡 실행별).
-- 성공한 태스크 실행만 필터링하고, 실행 시간 범위를 집계한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW task_run_stats_mv
AS
SELECT
  workspace_id,
  cluster_id,
  job_id,
  run_id,
  job_run_id,
  task_key,
  MIN(period_start_time)            AS period_start_time,
  MAX(period_end_time)              AS period_end_time,
  COLLECT_SET(result_state)         AS result_states,
  COLLECT_SET(termination_code)     AS termination_codes
FROM exploded_task_runs_st
GROUP BY workspace_id, job_id, run_id, job_run_id, task_key, cluster_id
HAVING ARRAY_CONTAINS(COLLECT_SET(result_state), 'SUCCESS')
    OR ARRAY_CONTAINS(COLLECT_SET(termination_code), 'SUCCESS');


-- =================================================================
-- 7. job_config_latest_mv  [MV]
-- =================================================================
-- 잡 메타데이터 (SCD2 최신 버전, 삭제되지 않은 잡만).
-- 잡별 가장 최근 구성 스냅샷을 제공한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW job_config_latest_mv
AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, job_id
      ORDER BY change_time DESC
    ) AS _rn
  FROM ${system_catalog}.${schema_lakeflow}.jobs
  WHERE workspace_id = '${workspace_id}'
    AND delete_time IS NULL
)
WHERE _rn = 1;


-- =================================================================
-- 8. cluster_job_cost_mv  [MV]
-- =================================================================
-- 클러스터별 × 잡별 비용 (DBU + USD).
-- billing.usage와 billing.list_prices를 조인하여 실제 금액을 산출한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW cluster_job_cost_mv
AS
SELECT
  u.workspace_id,
  u.usage_metadata.cluster_id                AS cluster_id,
  u.usage_metadata.job_id                    AS job_id,
  ROUND(SUM(u.usage_quantity), 4)            AS total_dbus,
  ROUND(SUM(
    u.usage_quantity
    * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)
  ), 2)                                      AS total_cost_usd
FROM ${system_catalog}.${schema_billing}.usage u
LEFT JOIN ${system_catalog}.${schema_billing}.list_prices lp
  ON  u.sku_name         = lp.sku_name
  AND u.cloud            = lp.cloud
  AND u.usage_start_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE u.workspace_id = '${workspace_id}'
  AND u.usage_metadata.cluster_id IS NOT NULL
  AND u.usage_date >= '${start_date}'
  AND u.usage_date <  '${end_date}'
GROUP BY u.workspace_id, u.usage_metadata.cluster_id, u.usage_metadata.job_id;


-- =================================================================
-- 9. instance_workload_profiles_mv  [MV]
-- =================================================================
-- 최종 결합: 인스턴스 활용률 + 클러스터 구성 + 잡 실행 + 비용.
-- 위 8개 파이프라인 오브젝트를 조인하여 분석용 최종 뷰를 생성한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW instance_workload_profiles_mv
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
  iu.max_cpu_util,
  iu.avg_cpu_wait,
  iu.max_cpu_wait,
  iu.avg_mem_util,
  iu.max_mem_util,
  iu.avg_net_mb_rec_minute,
  iu.avg_net_mb_sent_minute,
  iu.workload_profile,

  tr.job_id,
  j.name                   AS job_name,
  j.run_as,
  j.creator_id,
  tr.run_id,
  tr.job_run_id,
  tr.task_key,
  tr.period_start_time,
  tr.period_end_time,
  DATEDIFF(MINUTE, tr.period_start_time, tr.period_end_time) AS task_run_duration_minutes,
  tr.result_states,
  tr.termination_codes,

  COALESCE(cjc.total_dbus, 0)     AS total_dbus,
  COALESCE(cjc.total_cost_usd, 0) AS total_cost_usd

FROM instance_utilization_mv iu

LEFT JOIN node_count_stats_mv ncs
  ON  iu.cluster_id = ncs.cluster_id
  AND iu.node_type  = ncs.node_type

LEFT JOIN cluster_config_latest_mv cc
  ON  iu.cluster_id   = cc.cluster_id
  AND iu.workspace_id = cc.workspace_id

LEFT JOIN task_run_stats_mv tr
  ON  iu.cluster_id          = tr.cluster_id
  AND iu.workspace_id        = tr.workspace_id
  AND iu.instance_start_time <  tr.period_end_time
  AND tr.period_start_time   <  iu.instance_end_time

LEFT JOIN job_config_latest_mv j
  ON  tr.job_id       = j.job_id
  AND tr.workspace_id = j.workspace_id

LEFT JOIN cluster_job_cost_mv cjc
  ON  iu.cluster_id   = cjc.cluster_id
  AND iu.workspace_id = cjc.workspace_id
  AND tr.job_id       <=> cjc.job_id
;
