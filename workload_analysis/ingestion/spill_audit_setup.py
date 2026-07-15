# Databricks notebook source
# MAGIC %md
# MAGIC # Disk-Spill Audit — Setup & Event Ingestion (parameterized)
# MAGIC
# MAGIC Serverless-free, **fully parameterized** by bundle variables so it deploys to **any**
# MAGIC `catalog.schema` / workspace. Runs on a **classic single-node cluster** (no serverless).
# MAGIC
# MAGIC 1. Creates the target schema if missing.
# MAGIC 2. (Re)creates `query_spill_v` and `cluster_disk_pressure_v` **VIEWS** that read system
# MAGIC    tables directly (the dashboard / Pro SQL Warehouse queries these views live).
# MAGIC 3. Ensures the `expanded_disk_events` Delta table exists.
# MAGIC 4. Ingests actual `EXPANDED_DISK` / `DID_NOT_EXPAND_DISK` events from the Cluster Events
# MAGIC    API (not available in system tables) into that durable table.
# MAGIC
# MAGIC > Views are created via Python f-strings here because `CREATE VIEW` does not accept
# MAGIC > parameter markers; this keeps the catalog/schema/workspace_id fully bundle-driven
# MAGIC > with no per-customer SQL editing.

# COMMAND ----------

# MAGIC %md ## Parameters (← bundle variables)

# COMMAND ----------

dbutils.widgets.text("catalog", "hurcy", "Target catalog")
dbutils.widgets.text("schema", "test", "Target schema (analytics)")
dbutils.widgets.text("workspace_id", "984752964297111", "Workspace id to scope")
dbutils.widgets.text("lookback_days", "30", "Lookback window (days)")
dbutils.widgets.text("max_clusters", "150", "Max clusters to scan for events")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WORKSPACE_ID = dbutils.widgets.get("workspace_id")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
MAX_CLUSTERS = int(dbutils.widgets.get("max_clusters"))

FQ = f"{CATALOG}.{SCHEMA}"
EVENTS_TBL = f"{FQ}.expanded_disk_events"
print(f"target={FQ}  workspace_id={WORKSPACE_ID}  lookback={LOOKBACK_DAYS}d")

# COMMAND ----------

# MAGIC %md ## 1. Schema + spill views (read system tables directly, workspace-scoped)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

# --- query_spill_v : per-statement spill (Method A) ---
spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.query_spill_v (
  workspace_id COMMENT 'Workspace ID',
  statement_id COMMENT 'SQL statement ID',
  start_time COMMENT 'Statement start time',
  spill_date COMMENT 'Spill occurrence date (based on start_time)',
  compute_type COMMENT 'Original compute type code (WAREHOUSE / SERVERLESS_COMPUTE / CLASSIC_COMPUTE)',
  compute_label COMMENT 'Compute type label (SQL Warehouse / Serverless Compute / Classic Compute / Unknown)',
  is_job_attributed COMMENT 'Whether the statement is attributed to a job (job_id present)',
  warehouse_id COMMENT 'SQL Warehouse ID (when applicable)',
  cluster_id COMMENT 'Cluster ID (when applicable)',
  job_id COMMENT 'Job ID (when attributed to a job)',
  job_run_id COMMENT 'Job run ID',
  job_name COMMENT 'Job name (falls back to (job <id>) format when unknown)',
  statement_type COMMENT 'Statement type (SELECT/INSERT/REFRESH etc.)',
  executed_by COMMENT 'Statement executor',
  spill_gb COMMENT 'Disk spill (GB, spilled_local_bytes)',
  read_gb COMMENT 'Data read (GB, read_bytes)',
  shuffle_gb COMMENT 'Shuffle read (GB, shuffle_read_bytes)',
  read_files COMMENT 'Number of files read',
  pruned_files COMMENT 'Number of pruned (skipped) files',
  read_partitions COMMENT 'Number of partitions read',
  exec_s COMMENT 'Execution time (seconds)',
  root_cause COMMENT 'Spill root-cause classification (WIDE_SHUFFLE / NO_PRUNING / MV_REFRESH / MEMORY_BOUND / MODERATE)',
  root_cause_detail COMMENT 'Root-cause detail and tuning guide (in English)',
  statement_text COMMENT 'Statement text (whitespace-normalized then truncated to 400 chars)'
)
COMMENT 'Audit view of per-statement disk spill (spilled_local_bytes>0) based on system.query.history. Last lookback_days (default 30 days). Includes compute type label, job attribution, and root_cause classification. Source for the dashboard Disk Spill page.'
AS
WITH base AS (
  SELECT
    workspace_id, statement_id, start_time,
    CAST(start_time AS DATE)             AS spill_date,
    compute.type                         AS compute_type,
    compute.warehouse_id                 AS warehouse_id,
    compute.cluster_id                   AS cluster_id,
    query_source.job_info.job_id         AS job_id,
    query_source.job_info.job_run_id     AS job_run_id,
    statement_type, executed_by,
    spilled_local_bytes, read_bytes, shuffle_read_bytes,
    read_files, pruned_files, read_partitions, execution_duration_ms, statement_text
  FROM system.query.history
  WHERE workspace_id = '{WORKSPACE_ID}'
    AND start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
    AND spilled_local_bytes > 0
),
jn AS (
  SELECT job_id, name FROM (
    SELECT job_id, name,
           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
    WHERE workspace_id = '{WORKSPACE_ID}'
  ) WHERE rn = 1
)
SELECT
  b.workspace_id, b.statement_id, b.start_time, b.spill_date,
  b.compute_type,
  CASE b.compute_type
    WHEN 'WAREHOUSE'          THEN 'SQL Warehouse'
    WHEN 'SERVERLESS_COMPUTE' THEN 'Serverless Compute'
    WHEN 'CLASSIC_COMPUTE'    THEN 'Classic Compute'
    ELSE COALESCE(b.compute_type, 'Unknown')
  END                                       AS compute_label,
  (b.job_id IS NOT NULL)                    AS is_job_attributed,
  b.warehouse_id, b.cluster_id, b.job_id, b.job_run_id,
  CASE WHEN b.job_id IS NULL THEN NULL
       ELSE COALESCE(jn.name, CONCAT('(job ', b.job_id, ')')) END AS job_name,
  b.statement_type, b.executed_by,
  ROUND(b.spilled_local_bytes / 1e9, 2)     AS spill_gb,
  ROUND(b.read_bytes / 1e9, 2)              AS read_gb,
  ROUND(b.shuffle_read_bytes / 1e9, 2)      AS shuffle_gb,
  b.read_files, b.pruned_files, b.read_partitions,
  ROUND(b.execution_duration_ms / 1000.0, 0) AS exec_s,
  CASE
    WHEN b.shuffle_read_bytes > GREATEST(b.read_bytes, 1) * 1.2
     AND b.spilled_local_bytes > 0.5 * GREATEST(b.shuffle_read_bytes, 1) THEN 'WIDE_SHUFFLE'
    WHEN b.read_files >= 1000 AND COALESCE(b.pruned_files, 0) = 0        THEN 'NO_PRUNING'
    WHEN UPPER(COALESCE(b.statement_type, '')) = 'REFRESH'              THEN 'MV_REFRESH'
    WHEN b.spilled_local_bytes > GREATEST(b.read_bytes, 1)             THEN 'MEMORY_BOUND'
    ELSE 'MODERATE'
  END                                       AS root_cause,
  COALESCE(NULLIF(CONCAT_WS(' | ',
    CASE WHEN b.shuffle_read_bytes > GREATEST(b.read_bytes, 1) * 1.2
          AND b.spilled_local_bytes > 0.5 * GREATEST(b.shuffle_read_bytes, 1)
         THEN 'WIDE_SHUFFLE: shuffle>input & spill~shuffle — dedup/join/agg shuffling more than memory holds. Fix: AQE skew join, raise spark.sql.shuffle.partitions, cluster/Z-order by join/dedup keys.' END,
    CASE WHEN b.read_files >= 1000 AND COALESCE(b.pruned_files, 0) = 0
         THEN 'NO_PRUNING: pruned_files=0 over many files — full scan. Fix: add partition/cluster predicate (for MV verify incremental refresh).' END,
    CASE WHEN UPPER(COALESCE(b.statement_type, '')) = 'REFRESH'
         THEN 'MV_REFRESH: materialized view refresh — confirm it qualifies for INCREMENTAL refresh. A full recompute re-scans the source each run.' END,
    CASE WHEN b.spilled_local_bytes > GREATEST(b.read_bytes, 1)
         THEN 'MEMORY_BOUND: spill exceeds input bytes — size up memory-optimized workers or increase shuffle partitions to shrink each task.' END
  ), ''), 'MODERATE: spill present but no dominant pattern — monitor / minor tuning.') AS root_cause_detail,
  SUBSTR(REGEXP_REPLACE(b.statement_text, '\\\\s+', ' '), 1, 400) AS statement_text
FROM base b
LEFT JOIN jn ON b.job_id = jn.job_id
""")

# --- cluster_disk_pressure_v : disk-free floor / swap (Method B) ---
spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.cluster_disk_pressure_v (
  workspace_id COMMENT 'Workspace ID',
  cluster_id COMMENT 'Cluster ID',
  sample_date COMMENT 'Observation date',
  samples COMMENT 'Number of minute-level samples for that date',
  avg_mem_pct COMMENT 'Average memory usage %',
  max_swap_pct COMMENT 'Maximum memory swap %',
  min_disk_free_gb COMMENT 'Minimum free disk (GB, daily minimum of the per-mount-point minimum)',
  mins_disk_low COMMENT 'Number of samples with free disk below 8GB (minutes)',
  pressure_signal COMMENT 'Pressure signal: DISK_PRESSURE(free<8GB) / MEM_SWAP(swap>0) / OK'
)
COMMENT 'View of per cluster x date disk-free floor / swap pressure based on system.compute.node_timeline. Last lookback_days (default 30 days). Captures elastic-disk pressure of non-SQL Spark jobs not seen in query.history. Source for the dashboard Disk Spill page.'
AS
WITH nt AS (
  SELECT
    workspace_id, cluster_id,
    CAST(start_time AS DATE)                                       AS sample_date,
    mem_used_percent, mem_swap_percent,
    array_min(map_values(disk_free_bytes_per_mount_point)) / 1e9   AS min_disk_free_gb
  FROM system.compute.node_timeline
  WHERE workspace_id = '{WORKSPACE_ID}'
    AND disk_free_bytes_per_mount_point IS NOT NULL
    AND start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
)
SELECT
  workspace_id, cluster_id, sample_date,
  COUNT(*)                                                AS samples,
  ROUND(AVG(mem_used_percent), 0)                         AS avg_mem_pct,
  ROUND(MAX(mem_swap_percent), 1)                         AS max_swap_pct,
  ROUND(MIN(min_disk_free_gb), 2)                         AS min_disk_free_gb,
  SUM(CASE WHEN min_disk_free_gb < 8 THEN 1 ELSE 0 END)   AS mins_disk_low,
  CASE
    WHEN MIN(min_disk_free_gb) < 8 THEN 'DISK_PRESSURE'
    WHEN MAX(mem_swap_percent) > 0 THEN 'MEM_SWAP'
    ELSE 'OK'
  END                                                     AS pressure_signal
FROM nt
GROUP BY workspace_id, cluster_id, sample_date
""")

print(f"views ready: {FQ}.query_spill_v, {FQ}.cluster_disk_pressure_v")

# COMMAND ----------

# MAGIC %md ## 1b. cluster_names lookup (cluster_id → cluster_name)
# MAGIC Used by the Disk Pressure & EXPANDED_DISK dashboard widgets to show cluster names.
# MAGIC Prefers `system.compute.clusters`; if that is unprovisioned in this workspace, falls
# MAGIC back to the Clusters REST API for the candidate clusters (so it never blocks the spill views).

# COMMAND ----------

try:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FQ}.cluster_names AS
        SELECT cluster_id, MAX_BY(cluster_name, change_time) AS cluster_name
        FROM system.compute.clusters
        WHERE workspace_id = '{WORKSPACE_ID}'
        GROUP BY cluster_id
    """)
    print(f"cluster_names: from system.compute.clusters ({spark.table(f'{FQ}.cluster_names').count()} rows)")
except Exception as e:
    print(f"[info] system.compute.clusters unavailable ({str(e)[:80]}); using Clusters API fallback")
    from databricks.sdk import WorkspaceClient as _WC
    from pyspark.sql.types import StructType, StructField, StringType
    _w = _WC()
    _cands = [r["cluster_id"] for r in spark.sql(f"""
        SELECT DISTINCT cluster_id FROM system.compute.node_timeline
        WHERE workspace_id = '{WORKSPACE_ID}'
          AND start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
        LIMIT {MAX_CLUSTERS}
    """).collect()]
    _rows = []
    for _cid in _cands:
        try:
            _c = _w.api_client.do("GET", "/api/2.0/clusters/get", body={"cluster_id": _cid})
            _rows.append((_cid, _c.get("cluster_name")))
        except Exception:
            pass
    _sc = StructType([StructField("cluster_id", StringType()), StructField("cluster_name", StringType())])
    spark.createDataFrame(_rows, _sc).write.mode("overwrite").saveAsTable(f"{FQ}.cluster_names")
    print(f"cluster_names: from Clusters API ({len(_rows)} rows)")

# cluster_names comments (table/column) — covers both the CTAS + API fallback paths (co-located)
spark.sql(f"COMMENT ON TABLE {FQ}.cluster_names IS 'cluster_id -> cluster_name mapping lookup. For displaying cluster names in the Disk Pressure / EXPANDED_DISK dashboard widgets. Prefers system.compute.clusters, falls back to the Clusters REST API when unprovisioned.'")
spark.sql(f"ALTER TABLE {FQ}.cluster_names ALTER COLUMN cluster_id COMMENT 'Cluster ID'")
spark.sql(f"ALTER TABLE {FQ}.cluster_names ALTER COLUMN cluster_name COMMENT 'Cluster name (latest value by change_time)'")

# COMMAND ----------

# MAGIC %md ## 2. Durable EXPANDED_DISK events table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TBL} (
  workspace_id       STRING    COMMENT 'Workspace ID',
  cluster_id         STRING    COMMENT 'Cluster ID',
  job_id             STRING    COMMENT 'Job ID running on this cluster at the event time (time-mapped via job_task_run_timeline, NULL when not attributed)',
  job_run_id         STRING    COMMENT 'Job run ID running on this cluster at the event time (NULL when not attributed)',
  event_type         STRING    COMMENT 'Event type: EXPANDED_DISK (expanded) or DID_NOT_EXPAND_DISK (could not expand)',
  event_time         TIMESTAMP COMMENT 'Event occurrence time (UTC)',
  event_date         DATE      COMMENT 'Event occurrence date (based on event_time)',
  instance_id        STRING    COMMENT 'Instance ID where the event occurred',
  previous_disk_size BIGINT    COMMENT 'Disk size before expansion (bytes)',
  disk_size          BIGINT    COMMENT 'Disk size after expansion (bytes)',
  free_space_bytes   BIGINT    COMMENT 'Free disk space at event time (bytes)',
  cause              STRING    COMMENT 'Expansion/non-expansion cause (event details.cause)',
  details_json       STRING    COMMENT 'Full raw event details (JSON string)',
  captured_at        TIMESTAMP COMMENT 'Time this row was ingested into this table',
  event_key          STRING    COMMENT 'Idempotent merge key (cluster_id|timestamp_ms|event_type|instance_id)'
)
USING DELTA
COMMENT 'Persistent log of actual disk expansion events (EXPANDED_DISK / DID_NOT_EXPAND_DISK) collected from the Cluster Events API. Accumulates beyond the API cluster-purge retention limit. Idempotent MERGE keyed on event_key.'
""")

# Add job-attribution columns to an already-deployed table if missing (idempotent — schema evolution).
_existing_cols = {f.name for f in spark.table(EVENTS_TBL).schema.fields}
if "job_id" not in _existing_cols:
    spark.sql(f"ALTER TABLE {EVENTS_TBL} ADD COLUMN job_id STRING AFTER cluster_id")
if "job_run_id" not in _existing_cols:
    spark.sql(f"ALTER TABLE {EVENTS_TBL} ADD COLUMN job_run_id STRING AFTER job_id")

# COMMAND ----------

# MAGIC %md ## 3. Candidate clusters — recent classic clusters, most disk-pressured first

# COMMAND ----------

candidates_df = spark.sql(f"""
WITH nt AS (
  SELECT cluster_id,
         array_min(map_values(disk_free_bytes_per_mount_point)) / 1e9 AS min_disk_free_gb
  FROM system.compute.node_timeline
  WHERE workspace_id = '{WORKSPACE_ID}'
    AND disk_free_bytes_per_mount_point IS NOT NULL
    AND start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
)
SELECT cluster_id, ROUND(MIN(min_disk_free_gb), 2) AS min_disk_free_gb
FROM nt GROUP BY cluster_id ORDER BY min_disk_free_gb ASC LIMIT {MAX_CLUSTERS}
""")
cluster_ids = [r["cluster_id"] for r in candidates_df.collect()]
print(f"candidate clusters: {len(cluster_ids)}")

# COMMAND ----------

# MAGIC %md ## 4. Pull events via the Cluster Events API (version-stable raw REST)

# COMMAND ----------

import json
import datetime as dt
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
cutoff_ms = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=LOOKBACK_DAYS)).timestamp() * 1000)

def fetch_events(cid):
    body = {"cluster_id": cid, "start_time": cutoff_ms,
            "event_types": ["EXPANDED_DISK", "DID_NOT_EXPAND_DISK"], "limit": 500}
    while True:
        resp = w.api_client.do("POST", "/api/2.0/clusters/events", body=body)
        for ev in resp.get("events", []) or []:
            yield ev
        nxt = resp.get("next_page")
        if not nxt:
            break
        body = nxt

rows = []
scanned = skipped_missing = errored = 0
for cid in cluster_ids:
    try:
        scanned += 1
        for d in fetch_events(cid):
            det = d.get("details", {}) or {}
            ts_ms = d.get("timestamp")
            etype = d.get("type")
            inst = det.get("instance_id")
            rows.append({
                "workspace_id": WORKSPACE_ID, "cluster_id": cid, "event_type": etype,
                "event_time": dt.datetime.fromtimestamp(ts_ms / 1000, dt.timezone.utc) if ts_ms else None,
                "instance_id": inst,
                "previous_disk_size": det.get("previous_disk_size"),
                "disk_size": det.get("disk_size"),
                "free_space_bytes": det.get("free_space") or det.get("free_space_bytes"),
                "cause": det.get("cause"),
                "details_json": json.dumps(det),
                "event_key": f"{cid}|{ts_ms}|{etype}|{inst}",
            })
    except Exception as e:
        msg = str(e).lower()
        if "does not exist" in msg or "not found" in msg:
            skipped_missing += 1
        else:
            errored += 1
            print(f"[warn] {cid}: {e}")

print(f"scanned={scanned}  purged/skipped={skipped_missing}  errored={errored}  events_found={len(rows)}")

# COMMAND ----------

# MAGIC %md ## 5. MERGE into the durable table (idempotent)

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

schema_struct = StructType([
    StructField("workspace_id", StringType()), StructField("cluster_id", StringType()),
    StructField("event_type", StringType()), StructField("event_time", TimestampType()),
    StructField("instance_id", StringType()), StructField("previous_disk_size", LongType()),
    StructField("disk_size", LongType()), StructField("free_space_bytes", LongType()),
    StructField("cause", StringType()), StructField("details_json", StringType()),
    StructField("event_key", StringType()),
])

# cluster_id -> (job_id, job_run_id) time mapping. The Cluster Events API does not give job attribution,
# so attach the job run whose compute_ids includes this cluster and whose run window contains event_time.
# (job clusters are ephemeral per run, so effectively 1:1; for all-purpose, on overlap prefer the most recently started run.)
run_map = spark.sql(f"""
  SELECT workspace_id, cluster_id, job_id, job_run_id,
         MIN(period_start_time) AS run_start,
         MAX(period_end_time)   AS run_end
  FROM (
    SELECT workspace_id, EXPLODE(compute_ids) AS cluster_id,
           job_id, job_run_id, period_start_time, period_end_time
    FROM system.lakeflow.job_task_run_timeline
    WHERE workspace_id = '{WORKSPACE_ID}'
      AND ARRAY_SIZE(compute_ids) > 0
      AND period_start_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
  )
  WHERE job_run_id IS NOT NULL
  GROUP BY workspace_id, cluster_id, job_id, job_run_id
""")

if rows:
    base = (spark.createDataFrame(rows, schema_struct)
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("captured_at", F.current_timestamp())
            .dropDuplicates(["event_key"]))
    # Attach the run whose window contains event_time. On overlap keep only the single most recently started run.
    _pick = Window.partitionBy("event_key").orderBy(F.col("run_start").desc())
    src = (base.alias("e")
           .join(run_map.alias("r"),
                 (F.col("e.workspace_id") == F.col("r.workspace_id")) &
                 (F.col("e.cluster_id")   == F.col("r.cluster_id")) &
                 (F.col("e.event_time")   >= F.col("r.run_start")) &
                 (F.col("e.event_time")   <= F.col("r.run_end")),
                 "left")
           .withColumn("_rn", F.row_number().over(_pick))
           .filter(F.col("_rn") == 1)
           .select("e.*", "r.job_id", "r.job_run_id"))
    src.createOrReplaceTempView("_incoming_expanded_disk")
    spark.sql(f"""
        MERGE INTO {EVENTS_TBL} t
        USING _incoming_expanded_disk s ON t.event_key = s.event_key
        WHEN MATCHED AND t.job_id IS NULL AND s.job_id IS NOT NULL
          THEN UPDATE SET t.job_id = s.job_id, t.job_run_id = s.job_run_id
        WHEN NOT MATCHED THEN INSERT (
          workspace_id, cluster_id, job_id, job_run_id, event_type, event_time, event_date, instance_id,
          previous_disk_size, disk_size, free_space_bytes, cause, details_json, captured_at, event_key
        ) VALUES (
          s.workspace_id, s.cluster_id, s.job_id, s.job_run_id, s.event_type, s.event_time, s.event_date, s.instance_id,
          s.previous_disk_size, s.disk_size, s.free_space_bytes, s.cause, s.details_json, s.captured_at, s.event_key
        )
    """)
    _attributed = src.filter(F.col("job_id").isNotNull()).count()
    print(f"merged {src.count()} distinct events ({_attributed} attributed to a job run)")
else:
    print("no events found in window — table left unchanged (panel degrades gracefully to empty)")

total = spark.table(EVENTS_TBL).count()
print(f"{EVENTS_TBL} now holds {total} total events")
