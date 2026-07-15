---
name: compute-cost-analysis
description: >-
  Analyze Databricks job compute cost in DBU and USD. Use for questions about how
  much jobs cost, DBU consumption, dollar spend, classic vs serverless cost split,
  top spenders, cost per job or per run, and cost trends over time. Reads
  job_run_cost_analysis_mv (billing.usage × list_prices joined to cluster/job
  metadata) built by the job-monitoring-setup skill from live system.* tables. If
  the table is missing, run job-monitoring-setup first.
---

# Compute Cost Analysis

Answer cost questions from `job_run_cost_analysis_mv`. **Read-only: SELECT only.**
Substitute the real `catalog`/`schema` for `{{catalog}}.{{schema}}` first.

## Table

`job_run_cost_analysis_mv` — one row per job run over the last ~90 days. Cost =
`usage_quantity × list price` (`system.billing.usage` × `system.billing.list_prices`).
Key columns: `is_serverless`, `job_id`, `job_name`, `job_run_id`, `cluster_id`,
`cluster_source`, `run_start_time`, `run_duration_minutes`, `total_dbus`,
`total_cost_usd`.

Both **classic** (JOBS + evenly-allocated ALL_PURPOSE) and **serverless** runs are
included; `is_serverless` splits them. This is the cost basis the `compute-right-sizing`
skill draws `total_cost_usd` / `estimated_savings_usd` from.

## Answer queries (open only the one you need)

| Question | Query file |
|---|---|
| Classic vs serverless — cost & DBU split | `queries/cost_by_compute_type.sql` |
| Which jobs cost the most? | `queries/top_spenders.sql` |
| Daily cost trend, classic vs serverless | `queries/classic_vs_serverless.sql` |

## Guardrails

- `SELECT` only; never DDL/DML.
- Costs are **list-price** (`system.billing.list_prices`), not customer-net — state that;
  never present as a quote. (`fe-cost-optimization-report` converts list→net via
  `gtm_silver.consumption_detail`; that mirror is not available in a customer workspace.)
- Serverless can't be right-sized — for serverless spend, focus on job efficiency /
  scheduling, and hand cost-reduction-by-resizing back to `compute-right-sizing` (classic only).
- ALL_PURPOSE cluster cost is allocated evenly across the distinct job runs on that
  cluster — per-job serverless/JOB numbers are exact; per-job shared-cluster numbers are allocations.
