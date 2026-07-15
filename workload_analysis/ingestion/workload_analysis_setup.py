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

# --- Tunable right-sizing thresholds (overridable as job / notebook parameters) ---
dbutils.widgets.text("min_runs", "3", "Min runs in 90d to judge sizing")
dbutils.widgets.text("downsize_cpu_avg", "20", "DEFINITE_DOWNSIZE: avg CPU% below")
dbutils.widgets.text("downsize_mem_avg", "30", "DEFINITE_DOWNSIZE: avg Mem% below")
dbutils.widgets.text("likely_cpu_avg", "30", "LIKELY_DOWNSIZE: avg CPU% below")
dbutils.widgets.text("likely_mem_avg", "40", "LIKELY_DOWNSIZE: avg Mem% below")
dbutils.widgets.text("upsize_mem_p95", "85", "CONSIDER_UPSIZE: Mem P95% above")
dbutils.widgets.text("upsize_cpu_p95", "85", "CONSIDER_UPSIZE: CPU P95% above")
dbutils.widgets.text("swap_pct", "0", "CONSIDER_UPSIZE: max swap% above")
dbutils.widgets.text("spill_gb", "50", "CONSIDER_UPSIZE: 90d spill GB above")
dbutils.widgets.text("iowait_pct", "10", "IO_BOTTLENECK: avg CPU iowait% above")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WORKSPACE_ID = dbutils.widgets.get("workspace_id")

# These threshold placeholders are substituted into SQL_SCRIPT at the end of this notebook.
THRESHOLDS = {k: dbutils.widgets.get(k) for k in [
    "min_runs", "downsize_cpu_avg", "downsize_mem_avg", "likely_cpu_avg",
    "likely_mem_avg", "upsize_mem_p95", "upsize_cpu_p95", "swap_pct",
    "spill_gb", "iowait_pct",
]}
print(f"target={CATALOG}.{SCHEMA}  workspace_id={WORKSPACE_ID}")
print("thresholds:", THRESHOLDS)

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
-- Node count statistics per cluster (avg / max / stddev).
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
-- Per-instance resource utilization + workload profile classification.
-- Includes median, stddev (for sizing judgment).
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
-- Multi-node: workers only (driver=FALSE). Single-node (no worker): include driver -> measure util.
WHERE driver = FALSE OR has_worker = 0
GROUP BY workspace_id, cluster_id, driver, node_type, instance_id;


-- =================================================================
-- 3b. cluster_spill_v / job_spill_v  [TEMPORARY VIEW]
-- =================================================================
-- Aggregates actual disk spill (spilled_local_bytes) from system.query.history.
-- Direct evidence of memory pressure (-> GC pressure / OOM). Because system tables have no
-- executor GC time column, spill + swap (node_timeline.mem_swap_percent) are used as
-- proxy signals for GC / memory pressure.
-- Note: query.history mostly covers the SQL/DataFrame execution path, so pure RDD spill may be
-- missing -> judge together with the always-present node_timeline swap signal.
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
-- Cluster configuration info (SCD2 latest version).
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
-- Task run -> cluster mapping.
-- EXPLODE the compute_ids array to unfold the task-to-cluster relationship as 1:N.
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
-- Aggregation at the Job Run level (per cluster x per job run).
-- Keeps only job runs that have at least one succeeded task.
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
   -- Fully successful runs only: has a SUCCEEDED task and not a single failed task.
   AND SIZE(ARRAY_INTERSECT(
         COLLECT_SET(result_state),
         ARRAY('FAILED','ERROR','TIMEDOUT','UPSTREAM_FAILED',
               'UPSTREAM_CANCELED','INTERNAL_ERROR','MAXIMUM_CONCURRENT_RUNS_REACHED')
       )) = 0;


-- =================================================================
-- 6b. job_run_cluster_map_v  [TEMPORARY VIEW]
-- =================================================================
-- Mapping of the cluster_id where a job task ran (includes time range).
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
-- Job metadata (SCD2 latest version, non-deleted jobs only).
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
-- Cost aggregation at the All-Purpose cluster level.
-- Because billing_origin_product = 'ALL_PURPOSE' has a NULL job_id,
-- aggregation is done at the cluster_id level only.
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
-- Distinct job_run count per All-Purpose cluster.
-- Used as the denominator when distributing cost evenly.
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
-- Cost at the job run level (DBU + USD).
-- The is_serverless column distinguishes serverless/classic cost.
--
-- Job Compute / Serverless (billing_origin_product = 'JOBS'):
--   usage_metadata has job_id and job_run_id, so they are used directly.
-- All-Purpose (billing_origin_product = 'ALL_PURPOSE'):
--   usage_metadata has a NULL job_id, so the cluster_id-level cost is
--   mapped to job_id/job_run_id via exploded_task_runs_v.
--   Cost is distributed evenly in proportion to the number of job_runs on that cluster.
-- =================================================================

CREATE OR REPLACE TEMPORARY VIEW cluster_job_run_cost_v
AS
-- Part 1: Job Compute / Serverless — usage_metadata has job_id
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

-- Part 2: All-Purpose — distribute cluster_id-level cost evenly per job_run
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
-- 9. instance_workload_analysis_mv  [TABLE] — final output
-- =================================================================
-- Instance-level profiling: utilization + cluster config + job runs.
-- Joins the TEMPORARY VIEWs and persists them as the final MV.
-- =================================================================

CREATE OR REPLACE TABLE instance_workload_analysis_mv
COMMENT 'Instance (node) level workload profiling. Final table joining per-instance CPU/memory/network utilization over the last 90-day window with cluster config snapshot and job run info. Workspace scoped.'
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
-- Time aggregation at the Job Run level (based on job_run_timeline).
-- Serverless jobs have no cluster_id/compute_ids, so they are not included in
-- task_run_stats_v, and job_run_timeline supplements the time info.
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
-- 10. job_run_cost_analysis_mv  [TABLE] — final output
-- =================================================================
-- Cost profile at the Job Run level.
-- Uses cluster_job_run_cost_v as the driving table, performing only 1:1 JOINs.
-- instance_utilization is aggregated directly in the downstream sizing MV.
-- =================================================================

CREATE OR REPLACE TABLE job_run_cost_analysis_mv
COMMENT 'Cost analysis at the Job Run level. Joins DBU/USD cost per cluster x job run (billing.usage x list_prices) with cluster config, job metadata, and run time. Distinguishes classic/serverless. Last 90-day window.'
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
-- 12. all_purpose_cluster_sizing_mv  [TABLE] — final output
-- =================================================================
-- Identify Right-Sizing targets at the All-Purpose cluster level.
-- Because an All-Purpose cluster is shared by multiple jobs,
-- per-job recommendations would be misleading.
-- Aggregate by cluster_id to generate recommendations from a whole-cluster perspective.
-- Extract only rows with cluster_source IN ('UI', 'API') from job_run_cost_analysis_mv.
-- =================================================================

CREATE OR REPLACE TABLE all_purpose_cluster_sizing_mv
COMMENT 'Right-Sizing recommendation at the All-Purpose (UI/API) cluster level. Because it is shared by multiple jobs, utilization and cost are aggregated by cluster_id and a top-down CASE computes sizing_recommendation and estimated savings. Thresholds are adjustable via job parameters.'
AS
WITH cluster_util AS (
  -- utilization aggregation at the cluster level (directly from instance_utilization_v)
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
  -- cluster run count over the whole analysis window (90 days). Sample gate for sizing judgment.
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

  -- utilization: direct aggregation at the cluster level (1:1 JOIN)
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
    WHEN apt.window_run_count >= ${min_runs}
     AND cu.median_cpu_util < 30
     AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'BURST_PATTERN'
    WHEN apt.window_run_count >= ${min_runs}
     AND cu.avg_cpu_util < ${downsize_cpu_avg}
     AND cu.avg_mem_util < ${downsize_mem_avg}
     AND cu.median_cpu_util < 15
     AND cu.avg_stddev_cpu < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN apt.window_run_count >= ${min_runs}
     AND cu.avg_cpu_util < ${likely_cpu_avg} AND cu.avg_mem_util < ${likely_mem_avg}
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN apt.window_run_count >= ${min_runs}
     AND (cu.p95_mem_util > ${upsize_mem_p95}
       OR COALESCE(cu.max_swap_pct, 0) > ${swap_pct}
       OR COALESCE(cs.spill_gb, 0) > ${spill_gb}
       OR (cu.p95_cpu_util > ${upsize_cpu_p95} AND cu.p95_mem_util > 60))
      THEN 'CONSIDER_UPSIZE'
    WHEN cu.avg_cpu_wait > ${iowait_pct}
      THEN 'IO_BOTTLENECK'
    WHEN cu.avg_cpu_util IS NULL OR cu.avg_mem_util IS NULL
      THEN 'NO_UTIL_DATA'
    WHEN COALESCE(apt.window_run_count, 0) < ${min_runs}
      THEN 'INSUFFICIENT_RUNS'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN apt.window_run_count >= ${min_runs}
     AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
     AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
      OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN apt.window_run_count >= ${min_runs}
     AND cu.avg_cpu_util < ${downsize_cpu_avg} AND cu.avg_mem_util < ${downsize_mem_avg}
     AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN apt.window_run_count >= ${min_runs}
     AND cu.avg_cpu_util < ${likely_cpu_avg} AND cu.avg_mem_util < ${likely_mem_avg}
     AND cu.median_cpu_util < 25 AND cu.median_mem_util < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN apt.window_run_count >= ${min_runs}
     AND (cu.p95_mem_util > ${upsize_mem_p95}
       OR COALESCE(cu.max_swap_pct, 0) > ${swap_pct}
       OR COALESCE(cs.spill_gb, 0) > ${spill_gb}
       OR (cu.p95_cpu_util > ${upsize_cpu_p95} AND cu.p95_mem_util > 60))
      THEN CONCAT('리소스 한계 근접. ',
        CASE
          WHEN COALESCE(cu.max_swap_pct, 0) > ${swap_pct}
            THEN CONCAT('메모리 스왑 발생(max ', cu.max_swap_pct, '%) — RAM 고갈. 메모리 최적화 인스턴스로 상향 강력 권고.')
          WHEN COALESCE(cs.spill_gb, 0) > ${spill_gb}
            THEN CONCAT('디스크 스필 ', cs.spill_gb, 'GB — 메모리 부족. 메모리 최적화 워커 또는 shuffle 파티션 증가 검토.')
          WHEN cu.p95_mem_util > ${upsize_mem_p95}
            THEN CONCAT('메모리 P95 ', cu.p95_mem_util, '% 지속 포화 — 메모리 상향 또는 워커 추가 권고.')
          ELSE CONCAT('CPU P95 ', cu.p95_cpu_util, '%/메모리 P95 ', cu.p95_mem_util, '% 지속 포화 — 워커 추가 또는 큰 인스턴스 검토.')
        END)
    WHEN cu.avg_cpu_wait > ${iowait_pct}
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    WHEN cu.avg_cpu_util IS NULL OR cu.avg_mem_util IS NULL
      THEN '활용률(CPU/Mem) 데이터 없음 — sizing 판정 보류. node_timeline 수집 여부 확인.'
    WHEN COALESCE(apt.window_run_count, 0) < ${min_runs}
      THEN '최근 90일 실행 3회 미만 — 표본 부족으로 sizing 판정 보류. 실행 누적 후 재평가.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN apt.window_run_count >= ${min_runs}
       AND cu.median_cpu_util < 30 AND cu.median_mem_util < 40
       AND (cu.avg_stddev_cpu > 20 OR cu.peak_cpu_util > 80
        OR  cu.avg_stddev_mem > 20 OR cu.peak_mem_util > 80)
        THEN 0
      WHEN apt.window_run_count >= ${min_runs}
       AND cu.avg_cpu_util < ${downsize_cpu_avg} AND cu.avg_mem_util < ${downsize_mem_avg}
       AND cu.median_cpu_util < 15 AND cu.avg_stddev_cpu < 15
        THEN SUM(cjc.total_cost_usd) * 0.5
      WHEN apt.window_run_count >= ${min_runs}
       AND cu.avg_cpu_util < ${likely_cpu_avg} AND cu.avg_mem_util < ${likely_mem_avg}
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
-- 13. job_compute_sizing_mv  [TABLE] — final output
-- =================================================================
-- Identify Right-Sizing targets at the Job Compute level.
-- For Job Compute, job and cluster map 1:1, so per-job recommendations are accurate.
-- Extract only rows with cluster_source = 'JOB' from job_run_cost_analysis_mv.
-- =================================================================

CREATE OR REPLACE TABLE job_compute_sizing_mv
COMMENT 'Right-Sizing recommendation at the Job Compute (cluster_source=JOB) level. Job-to-cluster is a 1:1 mapping, so per-job recommendations by job_id are accurate. Because the clusters are ephemeral, cluster_id/cluster_name are NULL.'
AS
WITH job_clusters AS (
  -- Job Compute creates an ephemeral cluster per run. Aggregate runs/cost by job_id
  -- (over the whole 90-day analysis window). run_count = number of runs within the window.
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
  -- Aggregate the util of all run-clusters the job used within the window, by job_id.
  -- Since ephemeral cluster = 1 run, this is effectively the average of per-run util.
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
    WHEN jc.run_count >= ${min_runs}
     AND ju.median_cpu_util < 30
     AND ju.median_mem_util < 40
     AND (ju.avg_stddev_cpu > 20 OR ju.peak_cpu_util > 80
      OR  ju.avg_stddev_mem > 20 OR ju.peak_mem_util > 80)
      THEN 'BURST_PATTERN'
    WHEN jc.run_count >= ${min_runs}
     AND ju.avg_cpu_util < ${downsize_cpu_avg}
     AND ju.avg_mem_util < ${downsize_mem_avg}
     AND ju.median_cpu_util < 15
     AND ju.avg_stddev_cpu < 15
      THEN 'DEFINITE_DOWNSIZE'
    WHEN jc.run_count >= ${min_runs}
     AND ju.avg_cpu_util < ${likely_cpu_avg} AND ju.avg_mem_util < ${likely_mem_avg}
     AND ju.median_cpu_util < 25 AND ju.median_mem_util < 35
      THEN 'LIKELY_DOWNSIZE'
    WHEN jc.run_count >= ${min_runs}
     AND (ju.p95_mem_util > ${upsize_mem_p95}
       OR COALESCE(ju.max_swap_pct, 0) > ${swap_pct}
       OR COALESCE(js.spill_gb, 0) > ${spill_gb}
       OR (ju.p95_cpu_util > ${upsize_cpu_p95} AND ju.p95_mem_util > 60))
      THEN 'CONSIDER_UPSIZE'
    WHEN ju.avg_cpu_wait > ${iowait_pct}
      THEN 'IO_BOTTLENECK'
    WHEN ju.avg_cpu_util IS NULL OR ju.avg_mem_util IS NULL
      THEN 'NO_UTIL_DATA'
    WHEN COALESCE(jc.run_count, 0) < ${min_runs}
      THEN 'INSUFFICIENT_RUNS'
    ELSE 'APPROPRIATE'
  END AS sizing_recommendation,

  CASE
    WHEN jc.run_count >= ${min_runs}
     AND ju.median_cpu_util < 30 AND ju.median_mem_util < 40
     AND (ju.avg_stddev_cpu > 20 OR ju.peak_cpu_util > 80
      OR  ju.avg_stddev_mem > 20 OR ju.peak_mem_util > 80)
      THEN 'Burst 패턴 감지: median은 낮지만 순간 부하가 높아 현재 크기 유지 권고. Autoscaling 활용 검토.'
    WHEN jc.run_count >= ${min_runs}
     AND ju.avg_cpu_util < ${downsize_cpu_avg} AND ju.avg_mem_util < ${downsize_mem_avg}
     AND ju.median_cpu_util < 15 AND ju.avg_stddev_cpu < 15
      THEN '워커 수 50% 감소 또는 더 작은 인스턴스 타입으로 전환 강력 권고. median/P95/분산 모두 매우 낮음.'
    WHEN jc.run_count >= ${min_runs}
     AND ju.avg_cpu_util < ${likely_cpu_avg} AND ju.avg_mem_util < ${likely_mem_avg}
     AND ju.median_cpu_util < 25 AND ju.median_mem_util < 35
      THEN '워커 수 30% 감소 또는 인스턴스 타입 축소 검토. 평균·median 활용률이 지속적으로 낮음.'
    WHEN jc.run_count >= ${min_runs}
     AND (ju.p95_mem_util > ${upsize_mem_p95}
       OR COALESCE(ju.max_swap_pct, 0) > ${swap_pct}
       OR COALESCE(js.spill_gb, 0) > ${spill_gb}
       OR (ju.p95_cpu_util > ${upsize_cpu_p95} AND ju.p95_mem_util > 60))
      THEN CONCAT('리소스 한계 근접. ',
        CASE
          WHEN COALESCE(ju.max_swap_pct, 0) > ${swap_pct}
            THEN CONCAT('메모리 스왑 발생(max ', ju.max_swap_pct, '%) — RAM 고갈. 메모리 최적화 인스턴스로 상향 강력 권고.')
          WHEN COALESCE(js.spill_gb, 0) > ${spill_gb}
            THEN CONCAT('디스크 스필 ', js.spill_gb, 'GB — 메모리 부족. 메모리 최적화 워커 또는 shuffle 파티션 증가 검토.')
          WHEN ju.p95_mem_util > ${upsize_mem_p95}
            THEN CONCAT('메모리 P95 ', ju.p95_mem_util, '% 지속 포화 — 메모리 상향 또는 워커 추가 권고.')
          ELSE CONCAT('CPU P95 ', ju.p95_cpu_util, '%/메모리 P95 ', ju.p95_mem_util, '% 지속 포화 — 워커 추가 또는 큰 인스턴스 검토.')
        END)
    WHEN ju.avg_cpu_wait > ${iowait_pct}
      THEN 'I/O 병목 감지. 스토리지 최적화 또는 EBS 성능 개선 권고.'
    WHEN ju.avg_cpu_util IS NULL OR ju.avg_mem_util IS NULL
      THEN '활용률(CPU/Mem) 데이터 없음 — sizing 판정 보류. node_timeline 수집 여부 확인.'
    WHEN COALESCE(jc.run_count, 0) < ${min_runs}
      THEN '최근 90일 실행 3회 미만 — 표본 부족으로 sizing 판정 보류. 실행 누적 후 재평가.'
    ELSE '현재 구성 적절.'
  END AS recommendation_detail,

  ROUND(
    CASE
      WHEN jc.run_count >= ${min_runs}
       AND ju.median_cpu_util < 30 AND ju.median_mem_util < 40
       AND (ju.avg_stddev_cpu > 20 OR ju.peak_cpu_util > 80
        OR  ju.avg_stddev_mem > 20 OR ju.peak_mem_util > 80)
        THEN 0
      WHEN jc.run_count >= ${min_runs}
       AND ju.avg_cpu_util < ${downsize_cpu_avg} AND ju.avg_mem_util < ${downsize_mem_avg}
       AND ju.median_cpu_util < 15 AND ju.avg_stddev_cpu < 15
        THEN jc.total_cost_usd * 0.5
      WHEN jc.run_count >= ${min_runs}
       AND ju.avg_cpu_util < ${likely_cpu_avg} AND ju.avg_mem_util < ${likely_mem_avg}
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
-- 11. right_sizing_analysis_mv  [TABLE] — backward-compatible UNION
-- =================================================================
-- UNION ALL of MV 12 (All-Purpose, per-cluster) + MV 13 (Job Compute, per-job).
-- Kept for backward compatibility of the dashboard (6.right_sizing_targets).
-- All-Purpose rows fill job_id/job_name with NULL.
-- Job Compute rows fill job_count with NULL.
-- =================================================================

CREATE OR REPLACE TABLE right_sizing_analysis_mv
COMMENT 'Backward-compatible combined table that UNIONs all_purpose_cluster_sizing_mv (per-cluster) + job_compute_sizing_mv (per-job). For the dashboard right_sizing_targets widget. compute_type distinguishes the two sources, and non-applicable columns (job_id/job_name for All-Purpose, job_count for Job Compute) are NULL.'
AS
-- All-Purpose: per-cluster, job-specific columns are NULL
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

-- Job Compute: per-job, job_count is NULL (unnecessary since 1:1 mapping)
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

-- ===== Column comments (co-located; CTAS cannot inline column comments) =====
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN workspace_id COMMENT 'Workspace ID (analysis scope)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN cluster_id COMMENT 'Cluster ID';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN cluster_name COMMENT 'Cluster name (system.compute.clusters SCD2 latest snapshot)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN cluster_source COMMENT 'Cluster creation source (UI/API=All-Purpose, JOB/PIPELINE etc.)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN owned_by COMMENT 'Cluster owner account';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN instance_id COMMENT 'Instance (node) ID';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN driver COMMENT 'Whether it is a driver node (TRUE=driver, FALSE=worker)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN node_type COMMENT 'Observed node instance type (cloud instance type name)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN driver_node_type COMMENT 'Configured driver node type';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN worker_node_type COMMENT 'Configured worker node type';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN configured_workers COMMENT 'Configured fixed worker count (fixed-size cluster, system.compute.clusters.worker_count)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN min_autoscale_workers COMMENT 'Autoscale minimum worker count';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN max_autoscale_workers COMMENT 'Autoscale maximum worker count';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN dbr_version COMMENT 'Databricks Runtime version';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN policy_id COMMENT 'Cluster policy ID';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN tags COMMENT 'Cluster user-defined tags (MAP)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN instance_start_time COMMENT 'Instance observation start time (MIN start_time within the 90-day window)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN instance_end_time COMMENT 'Instance observation end time (MAX end_time)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN avg_node_count COMMENT 'Average active worker count per minute (autoscale behavior analysis)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN max_node_count COMMENT 'Maximum active worker count per minute';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN stddev_node_count COMMENT 'Standard deviation of active worker count per minute';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN avg_cpu_util COMMENT 'CPU utilization average % (cpu_user + cpu_system)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN median_cpu_util COMMENT 'CPU utilization median % (P50)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN max_cpu_util COMMENT 'CPU utilization maximum %';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN stddev_cpu_util COMMENT 'CPU utilization standard deviation (variability)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN avg_cpu_wait COMMENT 'CPU I/O wait ratio average % (iowait)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN max_cpu_wait COMMENT 'CPU I/O wait ratio maximum %';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN avg_mem_util COMMENT 'Memory usage average %';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN median_mem_util COMMENT 'Memory usage median % (P50)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN max_mem_util COMMENT 'Memory usage maximum %';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN stddev_mem_util COMMENT 'Memory usage standard deviation (variability)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN avg_net_mb_rec_minute COMMENT 'Network received average per minute (MB)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN avg_net_mb_sent_minute COMMENT 'Network sent average per minute (MB)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN workload_profile COMMENT 'Workload profile classification (CPU Intensive / Memory Intensive / Balanced - High/Moderate Utilization / Under-utilized)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN job_id COMMENT 'Job ID that ran on this instance';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN job_name COMMENT 'Job name (system.lakeflow.jobs)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN run_as COMMENT 'Job run-as principal';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN creator_id COMMENT 'Job creator ID';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN job_run_id COMMENT 'Job run ID';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN run_start_time COMMENT 'Job run start time (alias of period_start_time)';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN period_start_time COMMENT 'Job run period start time';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN period_end_time COMMENT 'Job run period end time';
ALTER TABLE instance_workload_analysis_mv ALTER COLUMN job_run_duration_minutes COMMENT 'Job run duration in minutes = period_end - period_start';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN workspace_id COMMENT 'Workspace ID';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN cluster_id COMMENT 'Cluster ID';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN is_serverless COMMENT 'Whether it is Serverless compute (billing.usage.product_features.is_serverless)';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN cluster_name COMMENT 'Cluster name';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN cluster_source COMMENT 'Cluster creation source (UI/API/JOB etc.)';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN owned_by COMMENT 'Cluster owner account';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN worker_node_type COMMENT 'Configured worker node type';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN configured_workers COMMENT 'Configured fixed worker count';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN min_autoscale_workers COMMENT 'Autoscale minimum worker count';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN max_autoscale_workers COMMENT 'Autoscale maximum worker count';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN dbr_version COMMENT 'Databricks Runtime version';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN policy_id COMMENT 'Cluster policy ID';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN job_id COMMENT 'Job ID';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN job_name COMMENT 'Job name';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN run_as COMMENT 'Job run-as principal';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN creator_id COMMENT 'Job creator ID';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN job_run_id COMMENT 'Job run ID';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN run_start_time COMMENT 'Run start time (task_run_stats first, else supplemented by job_run_timeline)';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN run_end_time COMMENT 'Run end time';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN run_duration_minutes COMMENT 'Run duration in minutes';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN task_count COMMENT 'Distinct task count included in the run (based on successful runs)';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN total_dbus COMMENT 'Total DBUs consumed by the run';
ALTER TABLE job_run_cost_analysis_mv ALTER COLUMN total_cost_usd COMMENT 'Total run cost (USD, based on list price)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN workspace_id COMMENT 'Workspace ID';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN cluster_id COMMENT 'Cluster ID';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN cluster_name COMMENT 'Cluster name';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN cluster_source COMMENT 'Cluster creation source (UI or API)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN owned_by COMMENT 'Cluster owner account';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN worker_node_type COMMENT 'Configured worker node type';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN configured_workers COMMENT 'Configured fixed worker count';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN min_autoscale_workers COMMENT 'Autoscale minimum worker count';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN max_autoscale_workers COMMENT 'Autoscale maximum worker count';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN dbr_version COMMENT 'Databricks Runtime version';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN policy_id COMMENT 'Cluster policy ID';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN run_start_time COMMENT 'Most recent run start date (DATE)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN job_count COMMENT 'Distinct job count that shared this cluster';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN run_count COMMENT 'Distinct job run count';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_run_duration_minutes COMMENT 'Average run duration in minutes';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN stddev_run_duration_minutes COMMENT 'Run duration standard deviation in minutes';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_cpu_util COMMENT 'Cluster instance CPU utilization average %';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN median_cpu_util COMMENT 'CPU utilization median % (P50)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN p95_cpu_util COMMENT 'CPU utilization P95 % (sustained-load metric)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN peak_cpu_util COMMENT 'CPU utilization maximum % (instantaneous peak)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_stddev_cpu COMMENT 'Average of per-instance CPU standard deviation (variability)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_mem_util COMMENT 'Memory usage average %';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN median_mem_util COMMENT 'Memory usage median % (P50)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN p95_mem_util COMMENT 'Memory usage P95 % (sustained-load metric)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN peak_mem_util COMMENT 'Memory usage maximum % (instantaneous peak)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_stddev_mem COMMENT 'Average of per-instance memory standard deviation (variability)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_cpu_wait COMMENT 'CPU I/O wait ratio average % (iowait, IO_BOTTLENECK signal)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN max_swap_pct COMMENT 'Memory swap maximum % (RAM exhaustion signal, CONSIDER_UPSIZE)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN spill_gb COMMENT 'Total disk spill over the last 90 days (GB, system.query.history)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN avg_instance_count COMMENT 'Observed distinct instance count';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN total_dbus COMMENT 'Total DBUs consumed';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN total_cost_usd COMMENT 'Total cost (USD)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN sizing_recommendation COMMENT 'Right-Sizing verdict (BURST_PATTERN / DEFINITE_DOWNSIZE / LIKELY_DOWNSIZE / CONSIDER_UPSIZE / IO_BOTTLENECK / NO_UTIL_DATA / INSUFFICIENT_RUNS / APPROPRIATE). First match in the top-down CASE wins.';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN recommendation_detail COMMENT 'Verdict rationale and recommended-action detail (in Korean, states the actual cause such as swap/spill/memory/CPU)';
ALTER TABLE all_purpose_cluster_sizing_mv ALTER COLUMN estimated_savings_usd COMMENT 'Estimated savings when downsizing (USD, DEFINITE=50%/LIKELY=30%)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN workspace_id COMMENT 'Workspace ID';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN cluster_id COMMENT 'Cluster ID (NULL because Job Compute is ephemeral)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN cluster_name COMMENT 'Cluster name (NULL because of per-job aggregation)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN cluster_source COMMENT 'Cluster creation source (JOB)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN job_id COMMENT 'Job ID';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN job_name COMMENT 'Job name';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN owned_by COMMENT 'Cluster owner account';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN worker_node_type COMMENT 'Configured worker node type';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN configured_workers COMMENT 'Configured fixed worker count';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN min_autoscale_workers COMMENT 'Autoscale minimum worker count';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN max_autoscale_workers COMMENT 'Autoscale maximum worker count';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN dbr_version COMMENT 'Databricks Runtime version';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN policy_id COMMENT 'Cluster policy ID';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN run_start_time COMMENT 'Most recent run start date (DATE)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN run_count COMMENT 'Distinct job run count (90-day window)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_run_duration_minutes COMMENT 'Average run duration in minutes';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN stddev_run_duration_minutes COMMENT 'Run duration standard deviation in minutes';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_cpu_util COMMENT 'CPU utilization average %';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN median_cpu_util COMMENT 'CPU utilization median % (P50)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN p95_cpu_util COMMENT 'CPU utilization P95 % (sustained-load metric)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN peak_cpu_util COMMENT 'CPU utilization maximum % (instantaneous peak)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_stddev_cpu COMMENT 'Average of per-instance CPU standard deviation (variability)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_mem_util COMMENT 'Memory usage average %';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN median_mem_util COMMENT 'Memory usage median % (P50)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN p95_mem_util COMMENT 'Memory usage P95 % (sustained-load metric)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN peak_mem_util COMMENT 'Memory usage maximum % (instantaneous peak)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_stddev_mem COMMENT 'Average of per-instance memory standard deviation (variability)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_cpu_wait COMMENT 'CPU I/O wait ratio average % (iowait, IO_BOTTLENECK signal)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN max_swap_pct COMMENT 'Memory swap maximum % (RAM exhaustion signal, CONSIDER_UPSIZE)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN spill_gb COMMENT 'Total disk spill over the last 90 days (GB, per job, system.query.history)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN avg_instance_count COMMENT 'Observed distinct instance count';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN total_dbus COMMENT 'Total DBUs consumed';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN total_cost_usd COMMENT 'Total cost (USD)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN sizing_recommendation COMMENT 'Right-Sizing verdict (BURST_PATTERN / DEFINITE_DOWNSIZE / LIKELY_DOWNSIZE / CONSIDER_UPSIZE / IO_BOTTLENECK / NO_UTIL_DATA / INSUFFICIENT_RUNS / APPROPRIATE). First match in the top-down CASE wins.';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN recommendation_detail COMMENT 'Verdict rationale and recommended-action detail (in Korean, states the actual cause such as swap/spill/memory/CPU)';
ALTER TABLE job_compute_sizing_mv ALTER COLUMN estimated_savings_usd COMMENT 'Estimated savings when downsizing (USD, DEFINITE=50%/LIKELY=30%)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN workspace_id COMMENT 'Workspace ID';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN cluster_id COMMENT 'Cluster ID (NULL for Job Compute rows)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN cluster_name COMMENT 'Cluster name (NULL for Job Compute rows)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN cluster_source COMMENT 'Cluster creation source (UI/API/JOB)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN compute_type COMMENT 'Compute category: All-Purpose or Job Compute';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN job_id COMMENT 'Job ID (present only for Job Compute rows, NULL for All-Purpose)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN job_name COMMENT 'Job name (present only for Job Compute rows)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN job_count COMMENT 'Distinct job count that shared the cluster (All-Purpose rows only, NULL for Job Compute)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN owned_by COMMENT 'Cluster owner account';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN worker_node_type COMMENT 'Configured worker node type';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN configured_workers COMMENT 'Configured fixed worker count';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN min_autoscale_workers COMMENT 'Autoscale minimum worker count';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN max_autoscale_workers COMMENT 'Autoscale maximum worker count';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN dbr_version COMMENT 'Databricks Runtime version';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN policy_id COMMENT 'Cluster policy ID';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN run_start_time COMMENT 'Most recent run start date (DATE)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN run_count COMMENT 'Distinct job run count';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_run_duration_minutes COMMENT 'Average run duration in minutes';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN stddev_run_duration_minutes COMMENT 'Run duration standard deviation in minutes';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_cpu_util COMMENT 'CPU utilization average %';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN median_cpu_util COMMENT 'CPU utilization median % (P50)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN p95_cpu_util COMMENT 'CPU utilization P95 %';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN peak_cpu_util COMMENT 'CPU utilization maximum % (instantaneous peak)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_stddev_cpu COMMENT 'Average of per-instance CPU standard deviation (variability)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_cpu_wait COMMENT 'CPU I/O wait ratio average % (iowait)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN max_swap_pct COMMENT 'Memory swap maximum % (RAM exhaustion signal)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN spill_gb COMMENT 'Total disk spill over the last 90 days (GB)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_mem_util COMMENT 'Memory usage average %';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN median_mem_util COMMENT 'Memory usage median % (P50)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN p95_mem_util COMMENT 'Memory usage P95 %';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN peak_mem_util COMMENT 'Memory usage maximum % (instantaneous peak)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_stddev_mem COMMENT 'Average of per-instance memory standard deviation (variability)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN avg_instance_count COMMENT 'Observed distinct instance count';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN total_dbus COMMENT 'Total DBUs consumed';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN total_cost_usd COMMENT 'Total cost (USD)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN sizing_recommendation COMMENT 'Right-Sizing verdict (BURST_PATTERN / DEFINITE_DOWNSIZE / LIKELY_DOWNSIZE / CONSIDER_UPSIZE / IO_BOTTLENECK / NO_UTIL_DATA / INSUFFICIENT_RUNS / APPROPRIATE)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN recommendation_detail COMMENT 'Verdict rationale and recommended-action detail (in Korean)';
ALTER TABLE right_sizing_analysis_mv ALTER COLUMN estimated_savings_usd COMMENT 'Estimated savings when downsizing (USD)';
"""
SQL_SCRIPT = SQL_SCRIPT.replace("${workspace_id}", WORKSPACE_ID)
for _k, _v in THRESHOLDS.items():
    SQL_SCRIPT = SQL_SCRIPT.replace("${" + _k + "}", _v)

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
