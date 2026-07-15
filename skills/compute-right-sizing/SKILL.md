---
name: compute-right-sizing
description: >-
  Diagnose over- and under-provisioned Databricks compute and quantify savings.
  Use for questions about cluster or job right-sizing, "which clusters are
  oversized / wasting money", downsizing, upsizing, autoscaling, burst patterns,
  CPU/memory utilization, P95 saturation, memory swap, disk spill driving an
  upsize, I/O bottlenecks, and estimated dollar savings. Reads
  right_sizing_analysis_mv (and all_purpose_cluster_sizing_mv, job_compute_sizing_mv,
  instance_workload_analysis_mv, job_run_cost_analysis_mv) built by the
  job-monitoring-setup skill from live system.* tables. If those tables are
  missing, run job-monitoring-setup first.
---

# Compute Right-Sizing Analysis

Answer right-sizing questions from the pre-built analysis tables. **Read-only: SELECT
only.** Substitute the real `catalog`/`schema` (bundle vars `catalog` /
`analytics_schema`, defaults `hurcy` / `test`) for the `{{catalog}}.{{schema}}`
placeholders in the query files before running.

## Tables & grain

- **`right_sizing_analysis_mv`** — unified; `compute_type IN ('All-Purpose','Job Compute')`.
  **Read this by default** — it covers both shared clusters and job compute in one place.
- `all_purpose_cluster_sizing_mv` — per `cluster_id` (UI/API shared clusters only).
- `job_compute_sizing_mv` — per `job_id` (ephemeral JOB clusters, 1:1 job↔cluster).
- `instance_workload_analysis_mv` — per-node CPU/mem/net + `workload_profile` (drill-down).
- `job_run_cost_analysis_mv` — cost basis (`total_cost_usd`, `total_dbus`, `is_serverless`).

Serverless compute is intentionally excluded from right-sizing (you can't resize it);
for serverless spend use the `compute-cost-analysis` skill.

## sizing_recommendation taxonomy

The `sizing_recommendation` column is computed by a top-down CASE (first match wins).
Full logic + thresholds: **`references/sizing-taxonomy.md`**. Values:
`BURST_PATTERN`, `DEFINITE_DOWNSIZE`, `LIKELY_DOWNSIZE`, `CONSIDER_UPSIZE`,
`IO_BOTTLENECK`, `NO_UTIL_DATA`, `INSUFFICIENT_RUNS`, `APPROPRIATE`.
`estimated_savings_usd` = `total_cost_usd × 0.5` (DEFINITE) / `× 0.3` (LIKELY) / else 0.

## Answer queries (open only the one you need)

| Question | Query file |
|---|---|
| Biggest savings from downsizing? | `queries/top_downsize_candidates.sql` |
| What's under pressure / needs upsizing, and why? | `queries/upsize_pressure.sql` |
| Where is the opportunity concentrated (by owner)? | `queries/savings_by_owner.sql` |
| Deep-dive one cluster or job | `queries/cluster_detail.sql` |

## Output contract

Present each recommendation as **Problem / Action / $ Savings / Effort (S-M-L) /
Risk (L-M-H) / Docs (a real docs.databricks.com URL)** — see
`references/output-contract.md`. `recommendation_detail` is stored in Korean; translate
it into the user's language and map it into the contract fields.

## Guardrails

- `SELECT` only; never DDL/DML. To (re)build tables, use the `job-monitoring-setup` skill.
- Treat `INSUFFICIENT_RUNS` / `NO_UTIL_DATA` as "cannot judge yet", not "APPROPRIATE".
- Savings are **list-price** estimates (`system.billing.list_prices`), not customer quotes.
- For `CONSIDER_UPSIZE`, always name the driver (swap / spill / mem P95 / cpu P95) from
  `max_swap_pct`, `spill_gb`, `p95_mem_util`, `p95_cpu_util` — don't just say "upsize".
