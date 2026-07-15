---
name: monitoring-data-quality
description: >-
  Validate that the Databricks monitoring analysis tables reconcile and can be
  trusted. Use when asked "can I trust these numbers", "do the cost rollups tie
  out", "run the validation / data-quality checks", or after a refresh to confirm
  integrity. Runs reconciliation tests (T01–T17) over job_run_cost_analysis_mv,
  all_purpose_cluster_sizing_mv, job_compute_sizing_mv, right_sizing_analysis_mv,
  and instance_workload_analysis_mv built by the job-monitoring-setup skill. If
  those tables are missing, run job-monitoring-setup first.
---

# Monitoring Data-Quality Checks

Confirm the analysis tables reconcile before trusting a right-sizing or cost answer.
**Read-only: SELECT only.** Substitute the real `catalog`/`schema` for
`{{catalog}}.{{schema}}` in the query file first.

## What it checks

`queries/run_all_tests.sql` emits `test_name | result (PASS/FAIL) | detail` for T01–T17:

- **Cost split** (T01–T04): attributable classic cost = All-Purpose + Job Compute; no
  uncovered `cluster_source`; per-source filter accuracy.
- **Serverless/classic partition** (T05–T06): total = serverless + classic; serverless
  correctly excluded from sizing.
- **Union parity** (T07–T09): `right_sizing_analysis_mv` cost / row count / by-compute_type
  match the two source tables.
- **Duration rollups** (T10–T12): `SUM(run_duration)` ≈ `SUM(avg_dur × run_count)`
  (5% / 10-min tolerance for rounding drift).
- **Sanity** (T13–T17): no negative cost/DBU/duration, no NULL keys (classic), all MVs non-empty.

## How to run

Substitute catalog/schema and run the whole file on a SQL warehouse (or the
`compute-cost-analysis` / analysis warehouse). **All rows should read `PASS`.** For any
`FAIL`, read the `detail` column (it shows the two sides and the diff) and report which
reconciliation broke — a FAIL means the setup pipeline produced inconsistent aggregates,
so route to `job-monitoring-setup` to re-refresh, then re-run.

## Guardrails

- `SELECT` only; never DDL/DML.
- This file mirrors `workload_analysis/validation/run_all_tests.sql` (the source of
  truth). If the pipeline's tables/columns change, regenerate this copy from that file.
