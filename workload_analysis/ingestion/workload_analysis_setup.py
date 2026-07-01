# Databricks notebook source
# MAGIC %md
# MAGIC # Right-Sizing Analysis — Setup (non-SDP, classic cluster)
# MAGIC
# MAGIC Builds the right-sizing analysis TABLEs from **system tables read directly** — no
# MAGIC serverless, no SDP/DLT, no system-table copies. Runs on a **classic single-node
# MAGIC cluster**, parameterized by bundle variables (deploys to any catalog.schema/workspace).
# MAGIC
# MAGIC Outputs (in `${catalog}`.`${schema}`): `instance_workload_analysis_mv`,
# MAGIC `job_run_cost_analysis_mv`, `all_purpose_cluster_sizing_mv`, `job_compute_sizing_mv`,
# MAGIC `right_sizing_analysis_mv` — same names the dashboard reads (no dashboard change).
# MAGIC
# MAGIC > Requires `system.compute.clusters` provisioned in the target workspace.

# COMMAND ----------

dbutils.widgets.text("catalog", "hurcy", "Target catalog")
dbutils.widgets.text("schema", "test", "Target schema (analytics)")
dbutils.widgets.text("workspace_id", "984752964297111", "Workspace id to scope")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WORKSPACE_ID = dbutils.widgets.get("workspace_id")
print(f"target={CATALOG}.{SCHEMA}  workspace_id={WORKSPACE_ID}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

# COMMAND ----------

import re

SQL_SCRIPT = r"""-- =====================================================================
-- Job & Cluster Right-Sizing Analysis  (non-SDP, reads system.* directly)
-- =====================================================================
-- Ported from the serverless DLT pipeline: no SDP/serverless, no system-table
-- copies. Intermediate steps are Spark TEMPORARY VIEWs and final outputs are TABLEs
-- in the current catalog.schema (set via USE by the runner notebook).
-- Reads system.compute.{node_timeline,clusters}, system.lakeflow.{jobs,
-- job_task_run_timeline,job_run_timeline}, system.billing.{usage,list_prices}.
-- Window: rolling 90 days. Scoped to '${workspace_id}'.
-- =====================================================================


CREATE OR REPLACE TEMPORARY VIEW node_count_per_minute_v
AS
SELECT
  cluster_id,
  node_type,
  DATE_TRUNC('minute', start_time) AS minute_ts,
  COUNT(DISTINCT instance_id)      AS node_count
FROM system.compute.node_timeline
WHERE workspace_id = '${workspace_id}'
  AND driver = FALSE
  AND start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND start_time <  CURRENT_DATE()
GROUP BY cluster_id, node_type, DATE_TRUNC('minute', start_time);


-- =================================================================
-- 2. node_count_stats_v  [TEMPORARY VIEW]
-- =================================================================
-- 클러스터별 노드 수 통계 (평균/최대/표준편차).
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW node_count_stats_v
AS
SELECT
  cluster_id,
  node_type,
  ROUND(AVG(node_count), 2)    AS avg_node_count,
  ROUND(MAX(node_count), 2)    AS max_node_count,
  ROUND(STDDEV(node_count), 2) AS stddev_node_count
FROM node_count_per_minute_v
GROUP BY cluster_id, node_type;


-- =================================================================
-- 3. instance_utilization_v  [TEMPORARY VIEW]
-- =================================================================
-- 인스턴스별 리소스 활용률 + 워크로드 프로파일 분류.
-- median, stddev 포함 (sizing 판정용).
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW instance_utilization_v
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
  ROUND(PERCENTILE_APPROX(cpu_user_percent + cpu_system_percent, 0.95), 2) AS p95_cpu_util,
  ROUND(STDDEV(cpu_user_percent + cpu_system_percent), 2)                 AS stddev_cpu_util,
  ROUND(AVG(cpu_wait_percent), 2)                      AS avg_cpu_wait,
  ROUND(MAX(cpu_wait_percent), 2)                      AS max_cpu_wait,
  ROUND(AVG(mem_used_percent), 2)                      AS avg_mem_util,
  ROUND(PERCENTILE_APPROX(mem_used_percent, 0.5), 2)   AS median_mem_util,
  ROUND(MAX(mem_used_percent), 2)                      AS max_mem_util,
  ROUND(PERCENTILE_APPROX(mem_used_percent, 0.95), 2)  AS p95_mem_util,
  ROUND(MAX(mem_swap_percent), 2)                      AS max_swap_pct,
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
FROM (
  SELECT *,
         MAX(CASE WHEN driver = FALSE THEN 1 ELSE 0 END)
           OVER (PARTITION BY workspace_id, cluster_id) AS has_worker
  FROM system.compute.node_timeline
  WHERE workspace_id = '${workspace_id}'
    AND start_time >= DATE_SUB(CURRENT_DATE(), 90)
    AND start_time <  CURRENT_DATE()
)
-- 멀티노드: 워커(driver=FALSE)만. 싱글노드(워커 없음): 드라이버 포함 → util 측정.
WHERE driver = FALSE OR has_worker = 0
GROUP BY workspace_id, cluster_id, driver, node_type, instance_id;


-- =================================================================
-- 3b. cluster_spill_v / job_spill_v  [TEMPORARY VIEW]
-- =================================================================
-- 실제 디스크 스필(spilled_local_bytes)을 system.query.history에서 집계.
-- 메모리 부족(→ GC 압박 / OOM)의 직접 증거. system 테이블에 executor GC time
-- 컬럼이 없으므로, 스필 + 스왑(node_timeline.mem_swap_percent)을 GC/메모리
-- 압박의 대체 신호로 사용한다.
-- 주의: query.history는 SQL/DataFrame 실행 경로 위주라 순수 RDD 스필은 누락될
-- 수 있음 → 항상 존재하는 node_timeline 스왑 신호와 함께 판정한다.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW cluster_spill_v AS
SELECT workspace_id, cluster_id,
       ROUND(SUM(spilled_local_bytes) / 1e9, 2) AS spill_gb,
       COUNT(*)                                 AS spill_statements
FROM (
  SELECT workspace_id, compute.cluster_id AS cluster_id, spilled_local_bytes
  FROM system.query.history
  WHERE workspace_id = '${workspace_id}'
    AND start_time >= DATE_SUB(CURRENT_DATE(), 90)
    AND start_time <  CURRENT_DATE()
    AND spilled_local_bytes > 0
    AND compute.cluster_id IS NOT NULL
)
GROUP BY workspace_id, cluster_id;

CREATE OR REPLACE TEMPORARY VIEW job_spill_v AS
SELECT workspace_id, job_id,
       ROUND(SUM(spilled_local_bytes) / 1e9, 2) AS spill_gb,
       COUNT(*)                                 AS spill_statements
FROM (
  SELECT workspace_id, query_source.job_info.job_id AS job_id, spilled_local_bytes
  FROM system.query.history
  WHERE workspace_id = '${workspace_id}'
    AND start_time >= DATE_SUB(CURRENT_DATE(), 90)
    AND start_time <  CURRENT_DATE()
    AND spilled_local_bytes > 0
    AND query_source.job_info.job_id IS NOT NULL
)
GROUP BY workspace_id, job_id;


-- =================================================================
-- 4. cluster_config_latest_v  [TEMPORARY VIEW]
-- =================================================================
-- 클러스터 구성 정보 (SCD2 최신 버전).
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW cluster_config_latest_v
AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, cluster_id
      ORDER BY change_time DESC
    ) AS _rn
  FROM system.compute.clusters
  WHERE workspace_id = '${workspace_id}'
)
WHERE _rn = 1;


-- =================================================================
-- 5. exploded_task_runs_v  [TEMPORARY VIEW]
-- =================================================================
-- 태스크 실행 → 클러스터 매핑.
-- compute_ids 배열을 EXPLODE하여 태스크-클러스터 관계를 1:N으로 풀어낸다.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW exploded_task_runs_v
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
FROM system.lakeflow.job_task_run_timeline tr
WHERE tr.workspace_id = '${workspace_id}'
  AND ARRAY_SIZE(tr.compute_ids) > 0
  AND tr.period_start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND tr.period_start_time <  CURRENT_DATE();


-- =================================================================
-- 6. task_run_stats_v  [TEMPORARY VIEW]
-- =================================================================
-- Job Run 단위 집계 (클러스터별 × 잡 실행별).
-- 성공한 태스크가 하나라도 있는 잡 실행만 필터링한다.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW task_run_stats_v
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
FROM exploded_task_runs_v
GROUP BY workspace_id, cluster_id, job_id, job_run_id
HAVING ARRAY_CONTAINS(COLLECT_SET(result_state), 'SUCCEEDED')
   -- 온전히 성공한 run만: SUCCEEDED 태스크가 있고 실패 태스크가 하나도 없음.
   AND SIZE(ARRAY_INTERSECT(
         COLLECT_SET(result_state),
         ARRAY('FAILED','ERROR','TIMEDOUT','UPSTREAM_FAILED',
               'UPSTREAM_CANCELED','INTERNAL_ERROR','MAXIMUM_CONCURRENT_RUNS_REACHED')
       )) = 0;


-- =================================================================
-- 6b. job_run_cluster_map_v  [TEMPORARY VIEW]
-- =================================================================
-- Job의 Task가 실행된 cluster_id 매핑 (시간 범위 포함).
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW job_run_cluster_map_v
AS
SELECT
  workspace_id,
  cluster_id,
  job_id,
  job_run_id,
  MIN(period_start_time)            AS period_start_time,
  MAX(period_end_time)              AS period_end_time
FROM exploded_task_runs_v
GROUP BY workspace_id, cluster_id, job_id, job_run_id;


-- =================================================================
-- 7. job_config_latest_v  [TEMPORARY VIEW]
-- =================================================================
-- 잡 메타데이터 (SCD2 최신 버전, 삭제되지 않은 잡만).
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW job_config_latest_v
AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, job_id
      ORDER BY change_time DESC
    ) AS _rn
  FROM system.lakeflow.jobs
  WHERE workspace_id = '${workspace_id}'
    AND delete_time IS NULL
)
WHERE _rn = 1;


-- =================================================================
-- 8a. ap_cluster_cost_v  [TEMPORARY VIEW]
-- =================================================================
-- All-Purpose 클러스터 단위 비용 집계.
-- billing_origin_product = 'ALL_PURPOSE'는 job_id가 NULL이므로
-- cluster_id 단위로만 집계한다.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW ap_cluster_cost_v
AS
SELECT
  u.workspace_id,
  u.usage_metadata.cluster_id              AS cluster_id,
  ROUND(SUM(u.usage_quantity), 4)          AS total_dbus,
  ROUND(SUM(
    u.usage_quantity
    * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)
  ), 2)                                    AS total_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
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
-- 8b. ap_cluster_run_count_v  [TEMPORARY VIEW]
-- =================================================================
-- All-Purpose 클러스터별 distinct job_run 수.
-- 비용 균등 배분 시 분모로 사용한다.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW ap_cluster_run_count_v
AS
SELECT
  workspace_id,
  cluster_id,
  COUNT(DISTINCT CONCAT(job_id, '-', job_run_id)) AS run_count
FROM exploded_task_runs_v
GROUP BY workspace_id, cluster_id;


-- =================================================================
-- 8c. cluster_job_run_cost_v  [TEMPORARY VIEW]
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

CREATE OR REPLACE TEMPORARY VIEW cluster_job_run_cost_v
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
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
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
FROM ap_cluster_cost_v ac
JOIN ap_cluster_run_count_v rc
  ON  ac.workspace_id = rc.workspace_id
  AND ac.cluster_id   = rc.cluster_id
  AND rc.run_count > 0
JOIN (
  SELECT DISTINCT workspace_id, cluster_id, job_id, job_run_id
  FROM exploded_task_runs_v
) etr
  ON  ac.workspace_id = etr.workspace_id
  AND ac.cluster_id   = etr.cluster_id;


-- =================================================================
-- 9. instance_workload_analysis_mv  [TABLE] — 최종 출력
-- =================================================================
-- 인스턴스 수준 프로파일링: 활용률 + 클러스터 구성 + 잡 실행.
-- TEMPORARY VIEW들을 조인하여 최종 MV로 영속화한다.
-- =================================================================

CREATE OR REPLACE TABLE instance_workload_analysis_mv AS
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

FROM instance_utilization_v iu

LEFT JOIN node_count_stats_v ncs
  ON  iu.cluster_id = ncs.cluster_id
  AND iu.node_type  = ncs.node_type

LEFT JOIN cluster_config_latest_v cc
  ON  iu.cluster_id   = cc.cluster_id
  AND iu.workspace_id = cc.workspace_id

LEFT JOIN job_run_cluster_map_v jrc
  ON  iu.cluster_id          = jrc.cluster_id
  AND iu.workspace_id        = jrc.workspace_id
  AND iu.instance_start_time <  jrc.period_end_time
  AND jrc.period_start_time  <  iu.instance_end_time

LEFT JOIN job_config_latest_v j
  ON  jrc.job_id       = j.job_id
  AND jrc.workspace_id = j.workspace_id
;


-- =================================================================
-- 8b. job_run_timeline_v  [TEMPORARY VIEW]
-- =================================================================
-- Job Run 단위 시간 집계 (job_run_timeline 기반).
-- Serverless job은 cluster_id/compute_ids가 없어 task_run_stats_v에
-- 포함되지 않으므로, job_run_timeline에서 시간 정보를 보완한다.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW job_run_timeline_v
AS
SELECT
  workspace_id,
  job_id,
  run_id                               AS job_run_id,
  MIN(period_start_time)               AS period_start_time,
  MAX(period_end_time)                 AS period_end_time
FROM system.lakeflow.job_run_timeline
WHERE workspace_id = '${workspace_id}'
  AND period_start_time >= DATE_SUB(CURRENT_DATE(), 90)
  AND period_start_time <  CURRENT_DATE()
GROUP BY workspace_id, job_id, run_id;


-- =================================================================
-- 10. job_run_cost_analysis_mv  [TABLE] — 최종 출력
-- =================================================================
-- Job Run 단위 비용 프로파일.
-- cluster_job_run_cost_v를 driving table로 1:1 JOIN만 수행.
-- instance_utilization은 downstream sizing MV에서 직접 집계한다.
-- =================================================================

CREATE OR REPLACE TABLE job_run_cost_analysis_mv AS
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

FROM cluster_job_run_cost_v cjc

LEFT JOIN cluster_config_latest_v cc
  ON  cjc.cluster_id   = cc.cluster_id
  AND cjc.workspace_id = cc.workspace_id

LEFT JOIN job_config_latest_v j
  ON  cjc.job_id       = j.job_id
  AND cjc.workspace_id = j.workspace_id

LEFT JOIN task_run_stats_v tr
  ON  cjc.cluster_id   = tr.cluster_id
  AND cjc.workspace_id = tr.workspace_id
  AND cjc.job_id       = tr.job_id
  AND cjc.job_run_id   = tr.job_run_id

LEFT JOIN job_run_timeline_v jrt
  ON  cjc.job_id       = jrt.job_id
  AND cjc.job_run_id   = jrt.job_run_id
  AND cjc.workspace_id = jrt.workspace_id
;


-- =================================================================
-- 12. all_purpose_cluster_sizing_mv  [TABLE] — 최종 출력
-- =================================================================
-- All-Purpose 클러스터 단위 Right Sizing 대상 식별.
-- All-Purpose 클러스터는 여러 잡이 동일 클러스터를 공유하므로,
-- per-job recommendation은 오해를 유발한다.
-- cluster_id 기준으로 집계하여 클러스터 전체 관점에서 권고를 생성한다.
-- job_run_cost_analysis_mv에서 cluster_source IN ('UI', 'API') 행만 추출.
-- =================================================================

CREATE OR REPLACE TABLE all_purpose_cluster_sizing_mv AS
WITH cluster_util AS (
  -- cluster 단위 utilization 집계 (instance_utilization_v에서 직접)
  SELECT
    workspace_id, cluster_id,
    COUNT(DISTINCT instance_id)                     AS instance_count,
    ROUND(AVG(avg_cpu_util), 2)                     AS avg_cpu_util,
    ROUND(AVG(median_cpu_util), 2)                  AS median_cpu_util,
    ROUND(MAX(max_cpu_util), 2)                     AS peak_cpu_util,
    ROUND(MAX(p95_cpu_util), 2)                     AS p95_cpu_util,
    ROUND(AVG(stddev_cpu_util), 2)                  AS avg_stddev_cpu,
    ROUND(AVG(avg_cpu_wait), 2)                     AS avg_cpu_wait,
    ROUND(AVG(avg_mem_util), 2)                     AS avg_mem_util,
    ROUND(AVG(median_mem_util), 2)                  AS median_mem_util,
    ROUND(MAX(max_mem_util), 2)                     AS peak_mem_util,
    ROUND(MAX(p95_mem_util), 2)                     AS p95_mem_util,
    ROUND(MAX(max_swap_pct), 2)                     AS max_swap_pct,
    ROUND(AVG(stddev_mem_util), 2)                  AS avg_stddev_mem
  FROM instance_utilization_v
  GROUP BY workspace_id, cluster_id
),
cluster_run_totals AS (
  -- 분석 윈도우(90일) 전체 기준 cluster 실행 횟수. sizing 판정의 표본 게이트.
  SELECT workspace_id, cluster_id,
         COUNT(DISTINCT job_run_id) AS window_run_count
  FROM job_run_cost_analysis_mv
  WHERE is_serverless = false AND cluster_source IN ('UI', 'API')
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
  cu.p95_cpu_util                                            AS p95_cpu_util,
  cu.peak_cpu_util,
  cu.avg_stddev_cpu,
  cu.avg_mem_util,
  cu.median_mem_util,
  cu.p95_mem_util                                            AS p95_mem_util,
  cu.peak_mem_util,
  cu.avg_stddev_mem,
  cu.avg_cpu_wait,
  COALESCE(cu.max_swap_pct, 0)                               AS max_swap_pct,
  COALESCE(cs.spill_gb, 0)                                   AS spill_gb,
  COALESCE(cu.instance_count, 0)                              AS avg_instance_count,

  ROUND(SUM(cjc.total_dbus), 4)                               AS total_dbus,
  ROUND(SUM(cjc.total_cost_usd), 2)                           AS total_cost_usd,

  CASE
    WHEN apt.window_run_count >= 3
     AND cu.median_cpu_util < 30
     AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'BURST_PATTERN'
    WHEN apt.window_run_count >= 3
     AND cu.avg_cpu_util < 20
     AND cu.avg_mem_util < 30
     AND cu.median_cpu_util < 15
     AND cu.avg_stddev_cpu < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN apt.window_run_count >= 3
     AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN apt.window_run_count >= 3
     AND (cu.p95_mem_util > 85
       OR COALESCE(cu.max_swap_pct, 0) > 0
       OR COALESCE(cs.spill_gb, 0) > 50
       OR (cu.p95_cpu_util > 85 AND cu.p95_mem_util > 60))
      THEN 'CONSIDER_UPSIZE'
    WHEN cu.avg_cpu_wait > 10
      THEN 'IO_BOTTLENECK'
    WHEN cu.avg_cpu_util IS NULL OR cu.avg_mem_util IS NULL
      THEN 'NO_UTIL_DATA'
    WHEN COALESCE(apt.window_run_count, 0) < 3
      THEN 'INSUFFICIENT_RUNS'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN apt.window_run_count >= 3
     AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN apt.window_run_count >= 3
     AND cu.avg_cpu_util < 20 AND cu.avg_mem_util < 30
     AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN apt.window_run_count >= 3
     AND cu.avg_cpu_util < 30 AND cu.avg_mem_util < 40
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN apt.window_run_count >= 3
     AND (cu.p95_mem_util > 85
       OR COALESCE(cu.max_swap_pct, 0) > 0
       OR COALESCE(cs.spill_gb, 0) > 50
       OR (cu.p95_cpu_util > 85 AND cu.p95_mem_util > 60))
      THEN CONCAT('리소스 한계 근접. ',
        CASE
          WHEN COALESCE(cu.max_swap_pct, 0) > 0
            THEN CONCAT('메모리 스왑 발생(max ', cu.max_swap_pct, '%) — RAM 고갈. 메모리 최적화 인스턴스로 상향 강력 권고.')
          WHEN COALESCE(cs.spill_gb, 0) > 50
            THEN CONCAT('디스크 스필 ', cs.spill_gb, 'GB — 메모리 부족. 메모리 최적화 워커 또는 shuffle 파티션 증가 검토.')
          WHEN cu.p95_mem_util > 85
            THEN CONCAT('메모리 P95 ', cu.p95_mem_util, '% 지속 포화 — 메모리 상향 또는 워커 추가 권고.')
          ELSE CONCAT('CPU P95 ', cu.p95_cpu_util, '%/메모리 P95 ', cu.p95_mem_util, '% 지속 포화 — 워커 추가 또는 큰 인스턴스 검토.')
        END)
    WHEN cu.avg_cpu_wait > 10
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    WHEN cu.avg_cpu_util IS NULL OR cu.avg_mem_util IS NULL
      THEN '활용률(CPU/Mem) 데이터 없음 — sizing 판정 보류. node_timeline 수집 여부 확인.'
    WHEN COALESCE(apt.window_run_count, 0) < 3
      THEN '최근 90일 실행 3회 미만 — 표본 부족으로 sizing 판정 보류. 실행 누적 후 재평가.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN apt.window_run_count >= 3
       AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
       AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
        OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
        THEN 0
      WHEN apt.window_run_count >= 3
       AND cu.avg_cpu_util < 20 AND cu.avg_mem_util < 30
       AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
        THEN SUM(cjc.total_cost_usd) * 0.5
      WHEN apt.window_run_count >= 3
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
LEFT JOIN cluster_run_totals apt
  ON  cjc.workspace_id = apt.workspace_id
  AND cjc.cluster_id   = apt.cluster_id
LEFT JOIN cluster_spill_v cs
  ON  cjc.workspace_id = cs.workspace_id
  AND cjc.cluster_id   = cs.cluster_id
WHERE cjc.is_serverless = false
  AND cjc.cluster_source IN ('UI', 'API')
GROUP BY
  cjc.workspace_id, cjc.cluster_id, cjc.cluster_name, cjc.cluster_source,
  cjc.owned_by, cjc.worker_node_type,
  cjc.configured_workers, cjc.min_autoscale_workers, cjc.max_autoscale_workers,
  cjc.dbr_version, cjc.policy_id,
  CAST(cjc.run_start_time AS DATE),
  cu.avg_cpu_util, cu.median_cpu_util, cu.peak_cpu_util, cu.p95_cpu_util,
  cu.avg_stddev_cpu, cu.avg_cpu_wait,
  cu.avg_mem_util, cu.median_mem_util, cu.peak_mem_util, cu.p95_mem_util,
  cu.max_swap_pct, cs.spill_gb,
  cu.avg_stddev_mem, cu.instance_count,
  apt.window_run_count
;


-- =================================================================
-- 13. job_compute_sizing_mv  [TABLE] — 최종 출력
-- =================================================================
-- Job Compute 단위 Right Sizing 대상 식별.
-- Job Compute는 잡과 클러스터가 1:1 매핑이므로 per-job 권고가 정확하다.
-- job_run_cost_analysis_mv에서 cluster_source = 'JOB' 행만 추출.
-- =================================================================

CREATE OR REPLACE TABLE job_compute_sizing_mv AS
WITH job_clusters AS (
  -- Job Compute는 run마다 ephemeral cluster가 생성된다. run/비용을 job_id 단위로
  -- 집계한다(분석 윈도우 90일 전체). run_count = 윈도우 내 실행 횟수.
  SELECT
    workspace_id, job_id,
    MAX(job_name)                              AS job_name,
    MAX(cluster_source)                        AS cluster_source,
    MAX(owned_by)                              AS owned_by,
    MAX(worker_node_type)                      AS worker_node_type,
    MAX(configured_workers)                    AS configured_workers,
    MAX(min_autoscale_workers)                 AS min_autoscale_workers,
    MAX(max_autoscale_workers)                 AS max_autoscale_workers,
    MAX(dbr_version)                           AS dbr_version,
    MAX(policy_id)                             AS policy_id,
    MAX(run_start_time)                        AS run_start_time,
    COUNT(DISTINCT job_run_id)                 AS run_count,
    ROUND(AVG(run_duration_minutes), 2)        AS avg_run_duration_minutes,
    ROUND(STDDEV(run_duration_minutes), 2)     AS stddev_run_duration_minutes,
    ROUND(SUM(total_dbus), 4)                  AS total_dbus,
    ROUND(SUM(total_cost_usd), 2)              AS total_cost_usd
  FROM job_run_cost_analysis_mv
  WHERE is_serverless = false AND cluster_source = 'JOB'
  GROUP BY workspace_id, job_id
),
job_util AS (
  -- 잡이 윈도우 내 사용한 모든 run-cluster의 util을 job_id 단위로 집계.
  -- ephemeral cluster = 1 run 이므로 사실상 run별 util의 평균.
  SELECT
    c.workspace_id, c.job_id,
    COUNT(DISTINCT iu.instance_id)             AS instance_count,
    ROUND(AVG(iu.avg_cpu_util), 2)             AS avg_cpu_util,
    ROUND(AVG(iu.median_cpu_util), 2)          AS median_cpu_util,
    ROUND(MAX(iu.max_cpu_util), 2)             AS peak_cpu_util,
    ROUND(MAX(iu.p95_cpu_util), 2)             AS p95_cpu_util,
    ROUND(AVG(iu.stddev_cpu_util), 2)          AS avg_stddev_cpu,
    ROUND(AVG(iu.avg_cpu_wait), 2)             AS avg_cpu_wait,
    ROUND(AVG(iu.avg_mem_util), 2)             AS avg_mem_util,
    ROUND(AVG(iu.median_mem_util), 2)          AS median_mem_util,
    ROUND(MAX(iu.max_mem_util), 2)             AS peak_mem_util,
    ROUND(MAX(iu.p95_mem_util), 2)             AS p95_mem_util,
    ROUND(MAX(iu.max_swap_pct), 2)             AS max_swap_pct,
    ROUND(AVG(iu.stddev_mem_util), 2)          AS avg_stddev_mem
  FROM (
    SELECT DISTINCT workspace_id, job_id, cluster_id
    FROM job_run_cost_analysis_mv
    WHERE is_serverless = false AND cluster_source = 'JOB'
  ) c
  JOIN instance_utilization_v iu
    ON  iu.workspace_id = c.workspace_id
    AND iu.cluster_id   = c.cluster_id
  GROUP BY c.workspace_id, c.job_id
)
SELECT
  jc.workspace_id,
  CAST(NULL AS STRING)                                       AS cluster_id,
  CAST(NULL AS STRING)                                       AS cluster_name,
  jc.cluster_source,
  jc.job_id,
  jc.job_name,
  jc.owned_by,
  jc.worker_node_type,
  jc.configured_workers,
  jc.min_autoscale_workers,
  jc.max_autoscale_workers,
  jc.dbr_version,
  jc.policy_id,
  CAST(jc.run_start_time AS DATE)                            AS run_start_time,

  jc.run_count,
  jc.avg_run_duration_minutes,
  jc.stddev_run_duration_minutes,

  ju.avg_cpu_util,
  ju.median_cpu_util,
  ju.p95_cpu_util                                           AS p95_cpu_util,
  ju.peak_cpu_util,
  ju.avg_stddev_cpu,
  ju.avg_mem_util,
  ju.median_mem_util,
  ju.p95_mem_util                                           AS p95_mem_util,
  ju.peak_mem_util,
  ju.avg_stddev_mem,
  ju.avg_cpu_wait,
  COALESCE(ju.max_swap_pct, 0)                              AS max_swap_pct,
  COALESCE(js.spill_gb, 0)                                  AS spill_gb,
  COALESCE(ju.instance_count, 0)                             AS avg_instance_count,

  jc.total_dbus,
  jc.total_cost_usd,

  CASE
    WHEN jc.run_count >= 3
     AND ju.median_cpu_util < 30
     AND ju.median_mem_util < 40
     AND (ju.avg_stddev_cpu > 20 OR ju.peak_cpu_util > 80
      OR  ju.avg_stddev_mem > 20 OR ju.peak_mem_util > 80)
      THEN 'BURST_PATTERN'
    WHEN jc.run_count >= 3
     AND ju.avg_cpu_util < 20
     AND ju.avg_mem_util < 30
     AND ju.median_cpu_util < 15
     AND ju.avg_stddev_cpu < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN jc.run_count >= 3
     AND ju.avg_cpu_util < 30 AND ju.avg_mem_util < 40
     AND ju.median_cpu_util < 25 AND ju.median_mem_util < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN jc.run_count >= 3
     AND (ju.p95_mem_util > 85
       OR COALESCE(ju.max_swap_pct, 0) > 0
       OR COALESCE(js.spill_gb, 0) > 50
       OR (ju.p95_cpu_util > 85 AND ju.p95_mem_util > 60))
      THEN 'CONSIDER_UPSIZE'
    WHEN ju.avg_cpu_wait > 10
      THEN 'IO_BOTTLENECK'
    WHEN ju.avg_cpu_util IS NULL OR ju.avg_mem_util IS NULL
      THEN 'NO_UTIL_DATA'
    WHEN COALESCE(jc.run_count, 0) < 3
      THEN 'INSUFFICIENT_RUNS'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN jc.run_count >= 3
     AND ju.median_cpu_util < 30 AND ju.median_mem_util < 40
     AND (ju.avg_stddev_cpu > 20 OR ju.peak_cpu_util > 80
      OR  ju.avg_stddev_mem > 20 OR ju.peak_mem_util > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN jc.run_count >= 3
     AND ju.avg_cpu_util < 20 AND ju.avg_mem_util < 30
     AND ju.median_cpu_util < 15 AND ju.avg_stddev_cpu < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN jc.run_count >= 3
     AND ju.avg_cpu_util < 30 AND ju.avg_mem_util < 40
     AND ju.median_cpu_util < 25 AND ju.median_mem_util < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN jc.run_count >= 3
     AND (ju.p95_mem_util > 85
       OR COALESCE(ju.max_swap_pct, 0) > 0
       OR COALESCE(js.spill_gb, 0) > 50
       OR (ju.p95_cpu_util > 85 AND ju.p95_mem_util > 60))
      THEN CONCAT('리소스 한계 근접. ',
        CASE
          WHEN COALESCE(ju.max_swap_pct, 0) > 0
            THEN CONCAT('메모리 스왑 발생(max ', ju.max_swap_pct, '%) — RAM 고갈. 메모리 최적화 인스턴스로 상향 강력 권고.')
          WHEN COALESCE(js.spill_gb, 0) > 50
            THEN CONCAT('디스크 스필 ', js.spill_gb, 'GB — 메모리 부족. 메모리 최적화 워커 또는 shuffle 파티션 증가 검토.')
          WHEN ju.p95_mem_util > 85
            THEN CONCAT('메모리 P95 ', ju.p95_mem_util, '% 지속 포화 — 메모리 상향 또는 워커 추가 권고.')
          ELSE CONCAT('CPU P95 ', ju.p95_cpu_util, '%/메모리 P95 ', ju.p95_mem_util, '% 지속 포화 — 워커 추가 또는 큰 인스턴스 검토.')
        END)
    WHEN ju.avg_cpu_wait > 10
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    WHEN ju.avg_cpu_util IS NULL OR ju.avg_mem_util IS NULL
      THEN '활용률(CPU/Mem) 데이터 없음 — sizing 판정 보류. node_timeline 수집 여부 확인.'
    WHEN COALESCE(jc.run_count, 0) < 3
      THEN '최근 90일 실행 3회 미만 — 표본 부족으로 sizing 판정 보류. 실행 누적 후 재평가.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN jc.run_count >= 3
       AND ju.median_cpu_util < 30 AND ju.median_mem_util < 40
       AND (ju.avg_stddev_cpu > 20 OR ju.peak_cpu_util > 80
        OR  ju.avg_stddev_mem > 20 OR ju.peak_mem_util > 80)
        THEN 0
      WHEN jc.run_count >= 3
       AND ju.avg_cpu_util < 20 AND ju.avg_mem_util < 30
       AND ju.median_cpu_util < 15 AND ju.avg_stddev_cpu < 15
        THEN jc.total_cost_usd * 0.5
      WHEN jc.run_count >= 3
       AND ju.avg_cpu_util < 30 AND ju.avg_mem_util < 40
       AND ju.median_cpu_util < 25 AND ju.median_mem_util < 35
        THEN jc.total_cost_usd * 0.3
      ELSE 0
    END, 2
  ) AS estimated_savings_usd

FROM job_clusters jc
LEFT JOIN job_util ju
  ON  jc.workspace_id = ju.workspace_id
  AND jc.job_id       = ju.job_id
LEFT JOIN job_spill_v js
  ON  jc.workspace_id = js.workspace_id
  AND jc.job_id       = js.job_id
;


-- =================================================================
-- 11. right_sizing_analysis_mv  [TABLE] — 하위호환 UNION
-- =================================================================
-- MV 12 (All-Purpose, per-cluster) + MV 13 (Job Compute, per-job)의
-- UNION ALL. 대시보드(6.right_sizing_targets)의 하위호환성을 위해 유지.
-- All-Purpose 행은 job_id/job_name을 NULL로 채운다.
-- Job Compute 행은 job_count를 NULL로 채운다.
-- =================================================================

CREATE OR REPLACE TABLE right_sizing_analysis_mv AS
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
  max_swap_pct,
  spill_gb,
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
FROM all_purpose_cluster_sizing_mv

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
  max_swap_pct,
  spill_gb,
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
FROM job_compute_sizing_mv
;
"""
SQL_SCRIPT = SQL_SCRIPT.replace("${workspace_id}", WORKSPACE_ID)

# Strip full-line comments BEFORE splitting on ';' so a ';' inside a comment can't break the split.
SQL_SCRIPT = "\n".join(l for l in SQL_SCRIPT.split("\n") if not l.strip().startswith("--"))
stmts = SQL_SCRIPT.split(";")
ran = 0
for st in stmts:
    body = st.strip()
    if not body:
        continue
    m = re.search(r"(?:TEMPORARY VIEW|REPLACE TABLE)\s+(\w+)", body)
    label = m.group(1) if m else body[:40]
    print(f"-> {label}")
    spark.sql(body)
    ran += 1
print(f"done: executed {ran} statements; 5 analysis tables in {CATALOG}.{SCHEMA}")

# COMMAND ----------

for t in ["instance_workload_analysis_mv", "job_run_cost_analysis_mv",
          "all_purpose_cluster_sizing_mv", "job_compute_sizing_mv", "right_sizing_analysis_mv"]:
    try:
        print(t, "rows:", spark.table(f"{CATALOG}.{SCHEMA}.{t}").count())
    except Exception as e:
        print(t, "ERROR:", str(e)[:120])
