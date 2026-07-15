# sizing_recommendation taxonomy

`sizing_recommendation` is computed once per cluster (All-Purpose) or per job (Job
Compute) by a **top-down CASE — first match wins**. Thresholds are job parameters
(set in `resources/jobs.yml` → `workload_analysis_setup.base_parameters`; overridable
per run). Defaults shown in ( ). "Gated" signals require `run_count ≥ min_runs`.

| Value | Trigger (defaults) | Gated? | Savings | Typical action |
|---|---|---|---|---|
| `NO_UTIL_DATA` | no utilization samples (`avg_cpu_util IS NULL`) | — | 0 | Cannot judge — check `system.compute.node_timeline` coverage. **Not** "appropriate". |
| `INSUFFICIENT_RUNS` | `run_count < min_runs` (3) | — | 0 | Cannot judge yet — too few runs in the 90-day window. |
| `BURST_PATTERN` | median CPU<30 & median mem<40, **and** `stddev_cpu > 20` OR `peak_cpu > 80` | yes | 0 | Spiky: don't downsize on averages. Consider autoscaling / job isolation. |
| `DEFINITE_DOWNSIZE` | avg CPU < `downsize_cpu_avg` (20) & avg mem < `downsize_mem_avg` (30), median CPU<15, stddev<15 | yes | ≈ **50%** of cost | Move to a smaller worker type / fewer workers. Strong, stable under-use. |
| `LIKELY_DOWNSIZE` | avg CPU < `likely_cpu_avg` (30) & avg mem < `likely_mem_avg` (40), median CPU<25 & median mem<35 | yes | ≈ **30%** of cost | Probably oversized — downsize one step and re-measure. |
| `CONSIDER_UPSIZE` | mem P95 > `upsize_mem_p95` (85) **OR** `max_swap_pct` > `swap_pct` (0) **OR** `spill_gb` > `spill_gb` (50) **OR** (CPU P95 > `upsize_cpu_p95` (85) AND mem P95 > 60) | yes | 0 | Under-provisioned. Name the driver: swap → more RAM; spill → memory-optimized type; CPU P95 → more/faster cores. |
| `IO_BOTTLENECK` | `avg_cpu_wait` > `iowait_pct` (10) | no | 0 | CPU stalls on I/O — network/storage bound, not size. Check data layout / shuffle / caching, not node size. |
| `APPROPRIATE` | none of the above | — | 0 | Leave as-is. |

Signal columns to cite when explaining a verdict: `avg_cpu_util`, `median_cpu_util`,
`p95_cpu_util`, `peak_cpu_util`, `avg_mem_util`, `p95_mem_util`, `max_swap_pct`,
`spill_gb`, `avg_cpu_wait`, `stddev_run_duration_minutes`. `recommendation_detail`
already spells out the actual driver (in Korean) — translate and reuse it.

## Grain notes

- **All-Purpose** (`all_purpose_cluster_sizing_mv`, `cluster_source IN ('UI','API')`):
  aggregated per `cluster_id`; many jobs may share one cluster (`job_count`), so a
  recommendation affects everyone on that cluster — call that out.
- **Job Compute** (`job_compute_sizing_mv`, `cluster_source='JOB'`): 1:1 job↔cluster,
  so per-job advice is precise; `cluster_id`/`cluster_name` are NULL by design.
- `right_sizing_analysis_mv` is the UNION of both with a `compute_type` discriminator.
