# sdp_optimization — Job & Cluster Workload Profiling Pipeline

A **Lakeflow Declarative Pipeline** that combines Databricks system tables (compute, billing, lakeflow) into a single analytical materialized view — `instance_workload_analysis_mv` — for job-cluster cost analysis and instance-level workload profiling.

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

## Configuration

The pipeline requires one parameter:

| Parameter | Type | Description |
|-----------|------|-------------|
| `workspace_id` | STRING | Target workspace ID to analyze |

## Directory Structure

```
sdp_optimization/
├── README.md
├── transformations/
│   └── instance_workload_analysis_mv.sql   # All 9 pipeline object definitions
└── explorations/
    └── sample_exploration.sql              # Ad-hoc notebook for verifying pipeline output
```

## Getting Started

1. Open the pipeline in the Databricks workspace.
2. Set the `workspace_id` pipeline parameter to the target workspace.
3. **Run pipeline** to materialize all 9 objects.
4. Use the exploration notebook under `explorations/` to verify output and perform ad-hoc analysis.
