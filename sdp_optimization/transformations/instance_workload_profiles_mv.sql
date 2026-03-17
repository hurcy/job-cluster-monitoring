-- =====================================================================
-- Job & Cluster Monitoring - Lakeflow Declarative Pipeline
-- =====================================================================
-- 시스템 테이블 사본(MV)에서 MV를 만드는 것을 피하기 위해,
-- 중간 단계(1~8)는 TEMPORARY LIVE VIEW로 처리하고,
-- 최종 출력(9, 10, 11)만 MATERIALIZED VIEW로 영속화한다.
--
-- Pipeline DAG:
--
--   ${system_catalog}.${schema_compute}.node_timeline  (외부 MV)
--     ├─ 1. node_count_per_minute_v        [TEMP VIEW]
--     │    └─ 2. node_count_stats_v        [TEMP VIEW]
--     └─ 3. instance_utilization_v         [TEMP VIEW]
--
--   ${system_catalog}.${schema_compute}.clusters       (외부 MV)
--     └─ 4. cluster_config_latest_v        [TEMP VIEW]
--
--   ${system_catalog}.${schema_lakeflow}.job_task_run_timeline (외부 MV)
--     └─ 5. exploded_task_runs_v           [TEMP VIEW]
--          ├─ 6. task_run_stats_v          [TEMP VIEW]
--          └─ 6b. job_run_cluster_map_v    [TEMP VIEW]
--
--   ${system_catalog}.${schema_lakeflow}.jobs          (외부 MV)
--     └─ 7. job_config_latest_v            [TEMP VIEW]
--
--   ${system_catalog}.${schema_billing}.usage + list_prices (외부 MV)
--     └─ 8. cluster_job_run_cost_v         [TEMP VIEW]
--
--   2,3,4,6b,7 ──►  9. instance_workload_profiles_mv   [MV] 최종 출력
--   8,4,7,6,3  ──► 10. job_run_cost_profiles_mv        [MV] 최종 출력
--   10         ──► 11. right_sizing_targets_mv          [MV] 최종 출력
--
-- Pipeline Configuration Parameters:
--   workspace_id      - 분석 대상 워크스페이스 ID (STRING)
--   system_catalog    - 시스템 테이블 카탈로그 (STRING, default: hurcy)
--   schema_compute    - compute 스키마 (STRING, default: compute)
--   schema_lakeflow   - lakeflow 스키마 (STRING, default: lakeflow)
--   schema_billing    - billing 스키마 (STRING, default: billing)
--   start_date        - 조회 시작일 (STRING, format: yyyy-MM-dd)
--   end_date          - 조회 종료일 (STRING, format: yyyy-MM-dd)
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
FROM ${system_catalog}.${schema_compute}.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= '${start_date}'
  AND start_time <  '${end_date}'
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
FROM ${system_catalog}.${schema_compute}.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= '${start_date}'
  AND start_time <  '${end_date}'
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
  FROM ${system_catalog}.${schema_compute}.clusters
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
FROM ${system_catalog}.${schema_lakeflow}.job_task_run_timeline tr
WHERE tr.workspace_id = '${workspace_id}'
  AND ARRAY_SIZE(tr.compute_ids) > 0
  AND tr.period_start_time >= '${start_date}'
  AND tr.period_start_time <  '${end_date}';


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
  FROM ${system_catalog}.${schema_lakeflow}.jobs
  WHERE workspace_id = '${workspace_id}'
    AND delete_time IS NULL
)
WHERE _rn = 1;


-- =================================================================
-- 8. cluster_job_run_cost_v  [TEMPORARY LIVE VIEW]
-- =================================================================
-- 잡 실행 단위 비용 (DBU + USD).
-- is_serverless 컬럼으로 serverless/classic 비용을 구분한다.
-- =================================================================

CREATE TEMPORARY LIVE VIEW cluster_job_run_cost_v
AS
SELECT
  u.workspace_id,
  u.usage_metadata.cluster_id                AS cluster_id,
  u.usage_metadata.job_id                    AS job_id,
  u.usage_metadata.job_run_id                AS job_run_id,
  u.product_features.is_serverless           AS is_serverless,
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
  AND u.usage_metadata.job_id IS NOT NULL
  AND u.usage_date >= '${start_date}'
  AND u.usage_date <  '${end_date}'
GROUP BY u.workspace_id, u.usage_metadata.cluster_id, u.usage_metadata.job_id, u.usage_metadata.job_run_id, u.product_features.is_serverless;


-- =================================================================
-- 9. instance_workload_profiles_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- 인스턴스 수준 프로파일링: 활용률 + 클러스터 구성 + 잡 실행.
-- TEMPORARY VIEW들을 조인하여 최종 MV로 영속화한다.
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
-- 10. job_run_cost_profiles_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- Job Run 단위 비용 프로파일.
-- TEMPORARY VIEW(8)를 driving table로 하여 다른 TEMPORARY VIEW들을
-- LEFT JOIN한다. Serverless 비용도 누락 없이 유지한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW job_run_cost_profiles_mv
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

  tr.period_start_time                                                   AS run_start_time,
  tr.period_end_time                                                     AS run_end_time,
  DATEDIFF(MINUTE, tr.period_start_time, tr.period_end_time)             AS run_duration_minutes,
  tr.task_count,

  COUNT(DISTINCT iu.instance_id)             AS instance_count,
  ROUND(AVG(iu.avg_cpu_util), 2)             AS avg_cpu_util,
  ROUND(AVG(iu.median_cpu_util), 2)          AS median_cpu_util,
  ROUND(MAX(iu.max_cpu_util), 2)             AS max_cpu_util,
  ROUND(AVG(iu.stddev_cpu_util), 2)          AS avg_stddev_cpu,
  ROUND(AVG(iu.avg_cpu_wait), 2)             AS avg_cpu_wait,
  ROUND(AVG(iu.avg_mem_util), 2)             AS avg_mem_util,
  ROUND(AVG(iu.median_mem_util), 2)          AS median_mem_util,
  ROUND(MAX(iu.max_mem_util), 2)             AS max_mem_util,
  ROUND(AVG(iu.stddev_mem_util), 2)          AS avg_stddev_mem,
  ROUND(AVG(iu.avg_net_mb_rec_minute), 2)    AS avg_net_mb_rec_minute,
  ROUND(AVG(iu.avg_net_mb_sent_minute), 2)   AS avg_net_mb_sent_minute,

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

LEFT JOIN LIVE.instance_utilization_v iu
  ON  cjc.cluster_id   = iu.cluster_id
  AND cjc.workspace_id = iu.workspace_id

GROUP BY
  cjc.workspace_id, cjc.cluster_id, cjc.is_serverless,
  cc.cluster_name, cc.cluster_source, cc.owned_by, cc.worker_node_type,
  cc.worker_count, cc.min_autoscale_workers, cc.max_autoscale_workers,
  cc.dbr_version, cc.policy_id,
  cjc.job_id, j.name, j.run_as, j.creator_id,
  cjc.job_run_id,
  tr.period_start_time, tr.period_end_time, tr.task_count,
  cjc.total_dbus, cjc.total_cost_usd
;


-- =================================================================
-- 11. right_sizing_targets_mv  [MATERIALIZED VIEW] — 최종 출력
-- =================================================================
-- Job × Cluster 단위 Right Sizing 대상 식별.
-- MV 10에서 classic compute만 추출, median + stddev + peak 기반 판정.
-- burst 패턴을 구분하여 오탐을 방지한다.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW right_sizing_targets_mv
AS
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

  CASE
    WHEN cjc.cluster_source IN ('UI', 'API') THEN 'All-Purpose'
    WHEN cjc.cluster_source = 'JOB' THEN 'Job Compute'
    ELSE COALESCE(cjc.cluster_source, 'Unknown')
  END AS compute_type,

  COUNT(DISTINCT cjc.job_run_id)                              AS run_count,
  ROUND(AVG(cjc.run_duration_minutes), 2)                     AS avg_run_duration_minutes,
  ROUND(STDDEV(cjc.run_duration_minutes), 2)                  AS stddev_run_duration_minutes,

  ROUND(AVG(cjc.avg_cpu_util), 2)                             AS avg_cpu_util,
  ROUND(AVG(cjc.median_cpu_util), 2)                          AS median_cpu_util,
  ROUND(PERCENTILE_APPROX(cjc.avg_cpu_util, 0.95), 2)        AS p95_cpu_util,
  ROUND(MAX(cjc.max_cpu_util), 2)                             AS peak_cpu_util,
  ROUND(AVG(cjc.avg_stddev_cpu), 2)                           AS avg_stddev_cpu,
  ROUND(AVG(cjc.avg_mem_util), 2)                             AS avg_mem_util,
  ROUND(AVG(cjc.median_mem_util), 2)                          AS median_mem_util,
  ROUND(PERCENTILE_APPROX(cjc.avg_mem_util, 0.95), 2)        AS p95_mem_util,
  ROUND(MAX(cjc.max_mem_util), 2)                             AS peak_mem_util,
  ROUND(AVG(cjc.avg_stddev_mem), 2)                           AS avg_stddev_mem,
  ROUND(AVG(cjc.avg_cpu_wait), 2)                             AS avg_cpu_wait,
  ROUND(AVG(cjc.instance_count), 2)                           AS avg_instance_count,

  ROUND(SUM(cjc.total_dbus), 4)                               AS total_dbus,
  ROUND(SUM(cjc.total_cost_usd), 2)                           AS total_cost_usd,

  CASE
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND AVG(cjc.median_cpu_util) < 30
     AND AVG(cjc.median_mem_util) < 40
     AND (AVG(cjc.avg_stddev_cpu) > 20 OR MAX(cjc.max_cpu_util) > 80
      OR  AVG(cjc.avg_stddev_mem) > 20 OR MAX(cjc.max_mem_util) > 80)
      THEN 'BURST_PATTERN'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND PERCENTILE_APPROX(cjc.avg_cpu_util, 0.95) < 20
     AND PERCENTILE_APPROX(cjc.avg_mem_util, 0.95) < 30
     AND AVG(cjc.median_cpu_util) < 15
     AND AVG(cjc.avg_stddev_cpu) < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND AVG(cjc.avg_cpu_util) < 30 AND AVG(cjc.avg_mem_util) < 40
     AND AVG(cjc.median_cpu_util) < 25 AND AVG(cjc.median_mem_util) < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND (PERCENTILE_APPROX(cjc.avg_cpu_util, 0.95) > 85
      OR  PERCENTILE_APPROX(cjc.avg_mem_util, 0.95) > 85)
      THEN 'CONSIDER_UPSIZE'
    WHEN AVG(cjc.avg_cpu_wait) > 10
      THEN 'IO_BOTTLENECK'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND AVG(cjc.median_cpu_util) < 30 AND AVG(cjc.median_mem_util) < 40
     AND (AVG(cjc.avg_stddev_cpu) > 20 OR MAX(cjc.max_cpu_util) > 80
      OR  AVG(cjc.avg_stddev_mem) > 20 OR MAX(cjc.max_mem_util) > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND PERCENTILE_APPROX(cjc.avg_cpu_util, 0.95) < 20
     AND PERCENTILE_APPROX(cjc.avg_mem_util, 0.95) < 30
     AND AVG(cjc.median_cpu_util) < 15 AND AVG(cjc.avg_stddev_cpu) < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND AVG(cjc.avg_cpu_util) < 30 AND AVG(cjc.avg_mem_util) < 40
     AND AVG(cjc.median_cpu_util) < 25 AND AVG(cjc.median_mem_util) < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
     AND (PERCENTILE_APPROX(cjc.avg_cpu_util, 0.95) > 85
      OR  PERCENTILE_APPROX(cjc.avg_mem_util, 0.95) > 85)
      THEN '리소스 한계 근접. 워커 추가 또는 더 큰 인스턴스 타입 검토 필요.'
    WHEN AVG(cjc.avg_cpu_wait) > 10
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND AVG(cjc.median_cpu_util) < 30 AND AVG(cjc.median_mem_util) < 40
       AND (AVG(cjc.avg_stddev_cpu) > 20 OR MAX(cjc.max_cpu_util) > 80
        OR  AVG(cjc.avg_stddev_mem) > 20 OR MAX(cjc.max_mem_util) > 80)
        THEN 0
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND PERCENTILE_APPROX(cjc.avg_cpu_util, 0.95) < 20
       AND PERCENTILE_APPROX(cjc.avg_mem_util, 0.95) < 30
       AND AVG(cjc.median_cpu_util) < 15 AND AVG(cjc.avg_stddev_cpu) < 15
        THEN SUM(cjc.total_cost_usd) * 0.5
      WHEN COUNT(DISTINCT cjc.job_run_id) >= 3
       AND AVG(cjc.avg_cpu_util) < 30 AND AVG(cjc.avg_mem_util) < 40
       AND AVG(cjc.median_cpu_util) < 25 AND AVG(cjc.median_mem_util) < 35
        THEN SUM(cjc.total_cost_usd) * 0.3
      ELSE 0
    END, 2
  ) AS estimated_savings_usd

FROM job_run_cost_profiles_mv cjc
WHERE cjc.is_serverless = false
GROUP BY
  cjc.workspace_id, cjc.job_id, cjc.job_name,
  cjc.cluster_id, cjc.cluster_name, cjc.cluster_source,
  cjc.owned_by, cjc.worker_node_type,
  cjc.configured_workers, cjc.min_autoscale_workers, cjc.max_autoscale_workers,
  cjc.dbr_version, cjc.policy_id
;
