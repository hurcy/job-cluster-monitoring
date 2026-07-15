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
  workspace_id COMMENT '워크스페이스 ID',
  statement_id COMMENT 'SQL statement ID',
  start_time COMMENT 'statement 시작 시각',
  spill_date COMMENT '스필 발생 일자 (start_time 기준)',
  compute_type COMMENT '원본 compute 타입 코드 (WAREHOUSE / SERVERLESS_COMPUTE / CLASSIC_COMPUTE)',
  compute_label COMMENT 'compute 타입 라벨 (SQL Warehouse / Serverless Compute / Classic Compute / Unknown)',
  is_job_attributed COMMENT '잡에 귀속된 statement 여부 (job_id 존재)',
  warehouse_id COMMENT 'SQL Warehouse ID (해당 시)',
  cluster_id COMMENT '클러스터 ID (해당 시)',
  job_id COMMENT '잡 ID (잡 귀속 시)',
  job_run_id COMMENT '잡 실행(run) ID',
  job_name COMMENT '잡 이름 (미확인 시 (job <id>) 형식)',
  statement_type COMMENT 'statement 유형 (SELECT/INSERT/REFRESH 등)',
  executed_by COMMENT 'statement 실행 주체',
  spill_gb COMMENT '디스크 스필 (GB, spilled_local_bytes)',
  read_gb COMMENT '읽은 데이터 (GB, read_bytes)',
  shuffle_gb COMMENT '셔플 읽기 (GB, shuffle_read_bytes)',
  read_files COMMENT '읽은 파일 수',
  pruned_files COMMENT '프루닝(스킵)된 파일 수',
  read_partitions COMMENT '읽은 파티션 수',
  exec_s COMMENT '실행 시간 (초)',
  root_cause COMMENT '스필 원인 분류 (WIDE_SHUFFLE / NO_PRUNING / MV_REFRESH / MEMORY_BOUND / MODERATE)',
  root_cause_detail COMMENT '원인 상세 및 튜닝 가이드 (영문)',
  statement_text COMMENT 'statement 텍스트 (공백 정규화 후 400자 축약)'
)
COMMENT 'system.query.history 기반 statement별 디스크 스필(spilled_local_bytes>0) 감사 뷰. 최근 lookback_days(기본 30일). compute 타입 라벨·잡 귀속·root_cause 분류 포함. 대시보드 Disk Spill 페이지 소스.'
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
  workspace_id COMMENT '워크스페이스 ID',
  cluster_id COMMENT '클러스터 ID',
  sample_date COMMENT '관측 일자',
  samples COMMENT '해당 일자 분 단위 샘플 수',
  avg_mem_pct COMMENT '평균 메모리 사용률 %',
  max_swap_pct COMMENT '최대 메모리 스왑 %',
  min_disk_free_gb COMMENT '최소 디스크 여유 (GB, 마운트포인트별 최솟값의 일자 최솟값)',
  mins_disk_low COMMENT '디스크 여유 8GB 미만 샘플 수 (분)',
  pressure_signal COMMENT '압박 신호: DISK_PRESSURE(여유<8GB) / MEM_SWAP(스왑>0) / OK'
)
COMMENT 'system.compute.node_timeline 기반 클러스터×일자 디스크 여유 하한 / 스왑 압박 뷰. 최근 lookback_days(기본 30일). query.history에 안 잡히는 비-SQL Spark 잡의 elastic-disk 압박 포착. 대시보드 Disk Spill 페이지 소스.'
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

# cluster_names 코멘트 (테이블/컬럼) — CTAS + API 폴백 두 경로 모두 커버 (co-located)
spark.sql(f"COMMENT ON TABLE {FQ}.cluster_names IS 'cluster_id → cluster_name 매핑 룩업. Disk Pressure / EXPANDED_DISK 대시보드 위젯에서 클러스터 이름 표시용. system.compute.clusters 우선, 미프로비저닝 시 Clusters REST API 폴백.'")
spark.sql(f"ALTER TABLE {FQ}.cluster_names ALTER COLUMN cluster_id COMMENT '클러스터 ID'")
spark.sql(f"ALTER TABLE {FQ}.cluster_names ALTER COLUMN cluster_name COMMENT '클러스터 이름 (change_time 기준 최신값)'")

# COMMAND ----------

# MAGIC %md ## 2. Durable EXPANDED_DISK events table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TBL} (
  workspace_id       STRING    COMMENT '워크스페이스 ID',
  cluster_id         STRING    COMMENT '클러스터 ID',
  job_id             STRING    COMMENT '이벤트 발생 시각에 이 클러스터에서 실행 중이던 잡 ID (job_task_run_timeline 시간 매핑, 미귀속 시 NULL)',
  job_run_id         STRING    COMMENT '이벤트 발생 시각에 이 클러스터에서 실행 중이던 잡 실행(run) ID (미귀속 시 NULL)',
  event_type         STRING    COMMENT '이벤트 유형: EXPANDED_DISK(확장됨) 또는 DID_NOT_EXPAND_DISK(확장 못함)',
  event_time         TIMESTAMP COMMENT '이벤트 발생 시각 (UTC)',
  event_date         DATE      COMMENT '이벤트 발생 일자 (event_time 기준)',
  instance_id        STRING    COMMENT '이벤트가 발생한 인스턴스 ID',
  previous_disk_size BIGINT    COMMENT '확장 전 디스크 크기 (bytes)',
  disk_size          BIGINT    COMMENT '확장 후 디스크 크기 (bytes)',
  free_space_bytes   BIGINT    COMMENT '이벤트 시점 여유 디스크 공간 (bytes)',
  cause              STRING    COMMENT '확장/미확장 사유 (이벤트 details.cause)',
  details_json       STRING    COMMENT '원본 이벤트 details 전체 (JSON 문자열)',
  captured_at        TIMESTAMP COMMENT '이 테이블에 수집된 시각',
  event_key          STRING    COMMENT '멱등 병합 키 (cluster_id|timestamp_ms|event_type|instance_id)'
)
USING DELTA
COMMENT 'Cluster Events API에서 수집한 실제 디스크 확장 이벤트(EXPANDED_DISK / DID_NOT_EXPAND_DISK) 영속 로그. API의 클러스터 purge 보존 한계를 넘어 누적. event_key 기준 멱등 MERGE.'
""")

# 기존 배포된 테이블에 job 귀속 컬럼이 없으면 추가 (idempotent — 스키마 진화).
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

# cluster_id → (job_id, job_run_id) 시간 매핑. Cluster Events API 는 job 귀속을 주지 않으므로,
# compute_ids 에 이 클러스터가 포함되고 event_time 이 실행 구간에 속하는 job run 을 붙인다.
# (job 클러스터는 run 당 ephemeral 이라 사실상 1:1; all-purpose 는 겹치면 가장 최근 시작 run 우선.)
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
    # event_time 이 run 윈도우 안에 드는 run 을 부착. 겹치면 가장 최근 시작 run 하나만 남긴다.
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
