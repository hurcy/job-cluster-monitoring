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
CREATE OR REPLACE VIEW {FQ}.query_spill_v AS
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
CREATE OR REPLACE VIEW {FQ}.cluster_disk_pressure_v AS
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

# MAGIC %md ## 2. Durable EXPANDED_DISK events table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TBL} (
  workspace_id        STRING,
  cluster_id          STRING,
  event_type          STRING,
  event_time          TIMESTAMP,
  event_date          DATE,
  instance_id         STRING,
  previous_disk_size  BIGINT,
  disk_size           BIGINT,
  free_space_bytes    BIGINT,
  cause               STRING,
  details_json        STRING,
  captured_at         TIMESTAMP,
  event_key           STRING
)
USING DELTA
COMMENT 'Actual cluster disk-expansion events captured from the Cluster Events API (durable log).'
""")

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

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

schema_struct = StructType([
    StructField("workspace_id", StringType()), StructField("cluster_id", StringType()),
    StructField("event_type", StringType()), StructField("event_time", TimestampType()),
    StructField("instance_id", StringType()), StructField("previous_disk_size", LongType()),
    StructField("disk_size", LongType()), StructField("free_space_bytes", LongType()),
    StructField("cause", StringType()), StructField("details_json", StringType()),
    StructField("event_key", StringType()),
])

if rows:
    src = (spark.createDataFrame(rows, schema_struct)
           .withColumn("event_date", F.to_date("event_time"))
           .withColumn("captured_at", F.current_timestamp())
           .dropDuplicates(["event_key"]))
    src.createOrReplaceTempView("_incoming_expanded_disk")
    spark.sql(f"""
        MERGE INTO {EVENTS_TBL} t
        USING _incoming_expanded_disk s ON t.event_key = s.event_key
        WHEN NOT MATCHED THEN INSERT (
          workspace_id, cluster_id, event_type, event_time, event_date, instance_id,
          previous_disk_size, disk_size, free_space_bytes, cause, details_json, captured_at, event_key
        ) VALUES (
          s.workspace_id, s.cluster_id, s.event_type, s.event_time, s.event_date, s.instance_id,
          s.previous_disk_size, s.disk_size, s.free_space_bytes, s.cause, s.details_json, s.captured_at, s.event_key
        )
    """)
    print(f"merged {src.count()} distinct events")
else:
    print("no events found in window — table left unchanged (panel degrades gracefully to empty)")

total = spark.table(EVENTS_TBL).count()
print(f"{EVENTS_TBL} now holds {total} total events")
