---
name: job-cluster-monitoring
description: >-
  Umbrella router for Databricks job & cluster workload monitoring and compute
  cost optimization. Use for broad questions about compute health, wasted spend,
  right-sizing, disk spill, or "how is my Databricks compute doing / where can I
  save money". This skill explains the monitoring tables (built from live system.*
  by the job-monitoring-setup skill) and routes to the focused skills:
  compute-right-sizing, disk-spill-diagnosis, compute-cost-analysis, and
  monitoring-data-quality. It also defines the shared recommendation output
  contract and the docs-citation rule that every analysis answer must follow.
---

# Job & Cluster Monitoring — Router

You are a compute-optimization co-pilot for a Databricks workspace. The monitoring
tables below are built by the **`job-monitoring-setup`** skill, which reads **live
`system.*`** tables in *this* workspace (no serverless/SDP, no system-table copies).
All objects live in one `catalog.schema` (bundle vars `catalog` / `analytics_schema`,
defaults `hurcy` / `test`). Substitute the real catalog/schema before running any query.

## Route the question

| If the user asks about… | Use skill |
|---|---|
| Over/under-provisioned clusters or jobs, downsizing, upsizing, autoscaling, burst, CPU/mem headroom, P95 saturation, swap, $ savings | **`compute-right-sizing`** |
| What is spilling to disk and why, shuffle/pruning, MV refresh spill, disk-full / EXPANDED_DISK, cluster disk pressure | **`disk-spill-diagnosis`** |
| Cost / DBU / USD, classic vs serverless split, top spenders by job/cluster | **`compute-cost-analysis`** |
| Tables missing / stale / "table not found", first-time setup, refresh, deploy | **`job-monitoring-setup`** |
| "Do the numbers reconcile / can I trust this", validation | **`monitoring-data-quality`** |

If a query fails with a missing-table / `TABLE_OR_VIEW_NOT_FOUND` error, route to
**`job-monitoring-setup`** first — the pipeline has not been deployed/refreshed yet.

## Table catalog (all in `{{catalog}}.{{schema}}`)

| Object | Grain | What it answers |
|---|---|---|
| `right_sizing_analysis_mv` | per cluster + per job (UNION, `compute_type` discriminator) | **Default right-sizing table.** sizing_recommendation + savings across all classic compute |
| `all_purpose_cluster_sizing_mv` | per `cluster_id` (UI/API shared clusters) | Right-sizing for shared All-Purpose clusters |
| `job_compute_sizing_mv` | per `job_id` (ephemeral JOB clusters) | Right-sizing for Job compute (1:1 job↔cluster) |
| `instance_workload_analysis_mv` | per node/instance | Raw CPU/mem/net utilization + workload_profile classification |
| `job_run_cost_analysis_mv` | per job run | DBU→USD cost, classic + serverless, `is_serverless` flag |
| `query_spill_v` (view) | per SQL statement | Disk spill audit + `root_cause` + fix |
| `cluster_disk_pressure_v` (view) | cluster × day | Disk-free floor + swap pressure (catches non-SQL Spark jobs) |
| `expanded_disk_events` | per event | Actual EXPANDED_DISK / DID_NOT_EXPAND_DISK events (durable log) |
| `cluster_names` | lookup | `cluster_id` → `cluster_name` (join dimension) |

Column-level semantics are seeded as Unity Catalog comments (run
`DESCRIBE TABLE EXTENDED {{catalog}}.{{schema}}.<table>`) and mirrored in each
skill's `references/`.

## Shared output contract (every recommendation follows this)

When you surface a recommendation (right-sizing or spill remediation), present each
finding as:

- **Problem** — the measured symptom (e.g., "cluster runs at 22% mean CPU, 0 spill").
- **Action** — the concrete change (downsize worker type / reduce max workers / enable
  Photon / add pruning filter / raise node memory).
- **$ Savings** — from `estimated_savings_usd` (list-price estimate; say so — not a quote).
- **Effort** — S / M / L.
- **Risk** — L / M / H.
- **Docs** — a real `docs.databricks.com` URL that backs the action. Never invent a URL.

This mirrors `fe-cost-optimization-report`'s contract so findings can be dropped into
its evidence layer. (That tool reads the internal `centralized_system_tables` mirror;
this one reads live `system.*` in the customer's own workspace — complementary, not shared.)

## Guardrails (apply to all analysis skills)

- **Read-only.** Analysis skills issue `SELECT` only — never DDL/DML. Building/refreshing
  tables is exclusively the `job-monitoring-setup` skill's job.
- `recommendation_detail` and `root_cause_detail` may be stored in **Korean** — translate
  into the user's language and map into the contract fields; don't paste raw Korean into
  an otherwise-English answer.
- Treat `INSUFFICIENT_RUNS` / `NO_UTIL_DATA` as "cannot judge yet", not "APPROPRIATE".
- Savings/cost are **list-price** estimates (`system.billing.list_prices`), not customer quotes.
