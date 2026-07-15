---
name: job-monitoring-setup
description: >-
  Deploy and refresh the Databricks job/cluster monitoring analysis tables and
  views that read live system.* tables. Use when the monitoring tables are
  missing, stale, or need re-running, when setting up cluster right-sizing /
  disk-spill / cost analysis for the first time, or when a query returns
  "table or view not found" for right_sizing_analysis_mv, job_run_cost_analysis_mv,
  all_purpose_cluster_sizing_mv, job_compute_sizing_mv, instance_workload_analysis_mv,
  query_spill_v, cluster_disk_pressure_v, or expanded_disk_events. Covers the DAB
  bundle deploy, the workload_monitoring_refresh job, tunable right-sizing
  thresholds, and re-seeding Unity Catalog object comments after each refresh.
---

# Job & Cluster Monitoring — Setup & Refresh

This is the only skill that **writes** — it builds/refreshes the tables the other
(read-only) analysis skills query. Everything is deployed via Databricks Asset Bundles
(DAB) from the repo root and reads **live `system.*`** in this workspace (no serverless
pipeline, no system-table copies). Detailed steps, thresholds, and troubleshooting:
**`references/deploy-runbook.md`**.

## What it builds (in `{{catalog}}.{{schema}}`, default `hurcy.test`)

- Right-sizing TABLEs: `instance_workload_analysis_mv`, `job_run_cost_analysis_mv`,
  `all_purpose_cluster_sizing_mv`, `job_compute_sizing_mv`, `right_sizing_analysis_mv`
- Disk-spill VIEWs + table: `query_spill_v`, `cluster_disk_pressure_v`, `expanded_disk_events`
- Lookup: `cluster_names`

## Prerequisites (verify first)

- Databricks CLI ≥ 0.218 authenticated; a profile in `~/.databrickscfg`.
- Unity Catalog + system-tables access. `system.compute.clusters` **must be provisioned**
  (right-sizing hard-requires it; disk-spill needs only `system.query.history` +
  `system.compute.node_timeline`).
- Smoke test: `SELECT 1 FROM system.compute.node_timeline LIMIT 1`.

## Runbook A — First-time setup (from the repo root)

1. Set bundle vars in `databricks.yml`: `catalog`, `analytics_schema`, `workspace_id`,
   `warehouse_id` (target the customer's own workspace).
2. `make deploy`  — runs `databricks bundle deploy` (jobs + dashboard) **and**
   `databricks workspace import-dir skills <SKILLS_ROOT>` (publishes these skills to
   `.assistant/skills/`). See the runbook for `SKILLS_ROOT` (workspace-wide vs personal).
3. `make refresh` — runs the `workload_monitoring_refresh` job to populate the tables.
4. Verify (below).

## Runbook B — Periodic refresh

- Scheduled daily 06:00 KST by the job. On demand: `make refresh`.
- The job has two parallel tasks: `workload_analysis_setup` (right-sizing) and
  `spill_audit_setup` (disk-spill; needs a live cluster for the Cluster Events API).

## Object comments (Genie metadata)

Table/column COMMENTs are declared **inline in the CREATE TABLE/VIEW statements** of the
two setup notebooks, so they are (re)applied atomically on every refresh — no separate
comment task or file. VIEWs (`query_spill_v`, `cluster_disk_pressure_v`) and the
explicit-schema `expanded_disk_events` carry column comments inline; the five CTAS
right-sizing tables carry an inline table COMMENT plus co-located `ALTER COLUMN … COMMENT`
right after each CREATE (CTAS cannot inline column comments). These comments are what make
Genie answers precise.

## Tunable right-sizing thresholds

Set on the job's `workload_analysis_setup` task (`base_parameters` in
`resources/jobs.yml`); override per run via the Jobs UI or `databricks bundle run
... --params`. Full table in `references/deploy-runbook.md`:
`min_runs`, `downsize_cpu_avg`, `downsize_mem_avg`, `likely_cpu_avg`, `likely_mem_avg`,
`upsize_mem_p95`, `upsize_cpu_p95`, `swap_pct`, `spill_gb`, `iowait_pct` (+ spill:
`lookback_days`, `max_clusters`).

## Verify

- Skills published: `databricks workspace list <SKILLS_ROOT>` shows the 6 skill folders.
- Tables populated: row counts > 0 for the five tables; then run the
  `monitoring-data-quality` skill (all T01–T17 = PASS).
- Comments seeded: `DESCRIBE TABLE EXTENDED {{catalog}}.{{schema}}.right_sizing_analysis_mv`
  shows table + column comments.
