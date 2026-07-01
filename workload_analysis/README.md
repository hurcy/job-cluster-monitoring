# Workload Analysis — Job & Cluster Right-Sizing (non-SDP)

`ingestion/workload_analysis_setup.py` — a **classic-cluster notebook (no serverless, no SDP/DLT)** —
combines Databricks system tables (compute, billing, lakeflow), read **directly** (no system-table
copies), into right-sizing analysis **tables** for job-cluster cost analysis and instance-level
workload profiling. Ported from the former serverless Lakeflow Declarative Pipeline: intermediate
steps are Spark `TEMPORARY VIEW`s and the final outputs are `TABLE`s (same names the dashboard reads).

> The DAG/objects below describe the logic (unchanged from the original). In the non-SDP port,
> objects suffixed conceptually as MV/ST are realized as in-session **temp views** (intermediates)
> and **tables** (the 5 final outputs). Requires `system.compute.clusters` provisioned.

## Pipeline DAG

```
system.compute.node_timeline
  ├─ 1. node_count_per_minute_mv   (per-minute worker counts)
  │    └─ 2. node_count_stats_mv   (avg / max / stddev)
  └─ 3. instance_utilization_mv    (CPU, memory, network + workload profile)

system.compute.clusters
  └─ 4. cluster_config_latest_mv   (SCD2 latest snapshot)

system.lakeflow.job_task_run_timeline
  └─ 5. exploded_task_runs_st      (Streaming Table — task ↔ cluster mapping)
       └─ 6. task_run_stats_mv     (per-run aggregates, successful only)

system.lakeflow.jobs
  └─ 7. job_config_latest_mv       (SCD2 latest snapshot, non-deleted)

system.billing.usage + list_prices
  └─ 8. cluster_job_cost_mv        (DBU + USD per cluster × job)

2, 3, 4, 6, 7, 8  ──►  9. instance_workload_analysis_mv
```

## Pipeline Objects

| # | Object | Type | Description |
|---|--------|------|-------------|
| 1 | `node_count_per_minute_mv` | MV | Per-minute active worker count per cluster (autoscale analysis) |
| 2 | `node_count_stats_mv` | MV | Worker-count statistics: avg, max, stddev per cluster |
| 3 | `instance_utilization_mv` | MV | Instance-level CPU / memory / network utilization with workload profile classification |
| 4 | `cluster_config_latest_mv` | MV | Latest cluster configuration snapshot (SCD2) |
| 5 | `exploded_task_runs_st` | Streaming Table | Explodes `compute_ids` array to map tasks to clusters incrementally |
| 6 | `task_run_stats_mv` | MV | Per-run aggregates filtered to successful task runs |
| 7 | `job_config_latest_mv` | MV | Latest job metadata snapshot (SCD2, non-deleted) |
| 8 | `cluster_job_cost_mv` | MV | Cost (DBU + USD) per cluster × job using billing + list prices |
| 9 | `instance_workload_analysis_mv` | MV | Final join combining utilization, config, runs, and cost |

### Workload Profile Categories (assigned in step 3)

| Profile | Condition |
|---------|-----------|
| CPU Intensive | avg CPU > 60 % and avg memory < 50 % |
| Memory Intensive | avg memory > 60 % and avg CPU < 50 % |
| Balanced - High Utilization | avg CPU > 60 % and avg memory > 60 % |
| Under-utilized | avg CPU < 30 % and avg memory < 40 % |
| Balanced - Moderate Utilization | everything else |

### Sizing Recommendation Signals (final tables)

`sizing_recommendation` (+ human-readable `recommendation_detail`) is assigned per cluster
(`all_purpose_cluster_sizing_mv`) or per job (`job_compute_sizing_mv`) by a **top-down CASE —
the first matching rule wins**. Percentiles come from `node_timeline` (**P95** = sustained
load, `peak` = true MAX); **spill** from `system.query.history` and **swap** from
`node_timeline`. Rules marked *(gated)* require **≥ 3 runs in the last 90 days**.

| Signal | Triggers when | Interpretation & action |
|--------|---------------|-------------------------|
| `BURST_PATTERN` *(gated)* | low median (CPU<30/mem<40) **but** high variance or peak >80 | Idle most of the time with occasional spikes. Keep the size and rely on **autoscaling** — do **not** downsize on averages. |
| `DEFINITE_DOWNSIZE` *(gated)* | avg CPU<20 & mem<30, median CPU<15, low variance | Clearly over-provisioned. Cut workers ~50 % or pick a smaller instance. (`estimated_savings_usd` ≈ 50 % of cost.) |
| `LIKELY_DOWNSIZE` *(gated)* | avg CPU<30 & mem<40, median CPU<25 & mem<35 | Consistently under-used. Cut workers ~30 % or shrink the instance. (savings ≈ 30 %.) |
| `CONSIDER_UPSIZE` *(gated)* | mem P95>85, **swap>0**, **spill>50 GB**, or (CPU P95>85 **and** mem P95>60) | Sustained saturation or real memory pressure. Add workers / move to a memory-optimized instance. `recommendation_detail` names the actual cause (swap / spill / memory / CPU). |
| `IO_BOTTLENECK` | avg CPU iowait >10 % | CPU sits idle waiting on disk — **not** a compute-sizing problem. Cross-check the Disk-Spill audit; prefer local-NVMe instances or faster storage (AWS EBS gp3 / Azure Managed Disk). |
| `NO_UTIL_DATA` | CPU/mem utilization is NULL | No `node_timeline` samples — verdict withheld. Verify metrics collection. |
| `INSUFFICIENT_RUNS` | < 3 runs in 90 days | Too few samples — verdict withheld until more runs accumulate. |
| `APPROPRIATE` | none of the above | Current configuration looks right; no action. |

> **Order matters.** A spiky workload is caught as `BURST_PATTERN` (checked first) and never
> mislabeled `DEFINITE_DOWNSIZE`. `IO_BOTTLENECK` has **no** run-count gate, so it can fire on
> low sample sizes — confirm the run count before acting.

## Disk-Spill Audit Objects (`ingestion/spill_audit_setup.py`)

Audits local-disk spill and elastic-disk pressure for Job Compute / Serverless / SQL
Warehouse workloads (source: `spill_audit_notebook.py`). **Serverless-free** — the spill
analysis layer is plain **SQL VIEWS** that read system tables directly (no DLT, no
serverless, no system-table copies); the Pro SQL Warehouse queries them live. Analysis
window: **last 30 days**, workspace-scoped.

| Object | Type | Source (read directly) | Description |
|--------|------|--------|-------------|
| `query_spill_v` | View | `system.query.history` | Per-statement spill (`spilled_local_bytes>0`) with compute-type label, job attribution, and `classify_cause` → `root_cause` / `root_cause_detail` tags. |
| `cluster_disk_pressure_v` | View | `system.compute.node_timeline` | Cluster×day disk-free floor + swap %; `pressure_signal` flags elastic-disk auto-expansion. Catches non-SQL Spark jobs invisible to `query.history`. |
| `expanded_disk_events` | Delta table | Cluster Events API (`/api/2.0/clusters/events`) | Durable log of actual `EXPANDED_DISK` / `DID_NOT_EXPAND_DISK` events — accumulates beyond the API's cluster-purge retention. |

`root_cause` ∈ {`WIDE_SHUFFLE`, `NO_PRUNING`, `MV_REFRESH`, `MEMORY_BOUND`, `MODERATE`}.
These feed the dashboard's **Disk Spill** and **Spill Detail & Expanded Disk** pages.

**Deployment** — `spill_audit_setup.py` (run by the `spill_audit_setup` task of the merged
`Workload Monitoring Refresh` job on a **classic single-node cluster**, no serverless) does
everything in one place, **parameterized by bundle
variables** (`catalog`, `schema`, `workspace_id`): creates the schema, (re)creates both views
(via Python f-strings, since `CREATE VIEW` rejects parameter markers), ensures the
`expanded_disk_events` table, then ingests events. Deploy to any catalog.schema/workspace by
changing the variables — **no per-customer SQL editing**. Retarget the dashboard with
`CATALOG=… SCHEMA=… OUT=… python3 build_dashboard.py` (repo root).

## Configuration

`workload_analysis_setup.py` notebook widgets (the `workload_analysis_setup` task of the
`Workload Monitoring Refresh` job supplies these via `base_parameters`):

| Widget | Default | Description |
|--------|---------|-------------|
| `catalog` | — | Output catalog for the analysis tables |
| `schema` | — | Output schema |
| `workspace_id` | — | Workspace ID used to scope every `system.*` read |
| `min_runs` | `3` | Min runs in 90d before a sizing verdict is issued |
| `downsize_cpu_avg` / `downsize_mem_avg` | `20` / `30` | `DEFINITE_DOWNSIZE`: avg CPU% / Mem% below |
| `likely_cpu_avg` / `likely_mem_avg` | `30` / `40` | `LIKELY_DOWNSIZE`: avg CPU% / Mem% below |
| `upsize_cpu_p95` / `upsize_mem_p95` | `85` / `85` | `CONSIDER_UPSIZE`: CPU / Mem **P95%** above |
| `swap_pct` | `0` | `CONSIDER_UPSIZE`: max swap% above (0 = any swap; raise to reduce false positives) |
| `spill_gb` | `50` | `CONSIDER_UPSIZE`: 90d disk-spill GB above |
| `iowait_pct` | `10` | `IO_BOTTLENECK`: avg CPU iowait% above |

> These thresholds are the ones tabulated in **Sizing Recommendation Signals** above. Override
> per-run from the Jobs UI (*Run now with different parameters*) or change the `base_parameters`
> defaults in `resources/jobs.yml`.

## Directory Structure

```
workload_analysis/
├── README.md
├── ingestion/
│   ├── workload_analysis_setup.py   # Right-Sizing tables — reads system.* directly, no SDP
│   └── spill_audit_setup.py         # Disk-Spill views + EXPANDED_DISK event ingestion
├── explorations/                    # ad-hoc notebooks
└── validation/                      # data-quality check queries
```

## Getting Started

1. Set bundle variables (`catalog`, `analytics_schema`, `workspace_id`, `warehouse_id`) in `databricks.yml`.
2. `databricks bundle deploy -t <target>`.
3. Run the merged analysis job (classic clusters — **no serverless, no SDP**):
   - `databricks bundle run workload_monitoring_refresh -t <target>` — runs both tasks:
     `workload_analysis_setup` (Right-Sizing tables, needs `system.compute.clusters`) and
     `spill_audit_setup` (Disk-Spill views + events).
4. Retarget the dashboard for a different catalog.schema: `CATALOG=… SCHEMA=… OUT=… python3 build_dashboard.py` (repo root).
