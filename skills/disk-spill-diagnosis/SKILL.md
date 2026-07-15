---
name: disk-spill-diagnosis
description: >-
  Diagnose Databricks disk spill and cluster disk/memory pressure. Use for
  questions about queries or jobs spilling to local disk, slow shuffles, wide
  shuffles, missing file pruning, materialized-view refresh spill, memory-bound
  statements, clusters running out of local disk, memory swap, and elastic-disk
  expansion (EXPANDED_DISK / DID_NOT_EXPAND_DISK) events. Reads query_spill_v,
  cluster_disk_pressure_v, expanded_disk_events, and cluster_names built by the
  job-monitoring-setup skill from live system.* tables. If those are missing, run
  job-monitoring-setup first.
---

# Disk-Spill & Disk-Pressure Diagnosis

Answer spill / disk-pressure questions from the pre-built views. **Read-only: SELECT
only.** Substitute the real `catalog`/`schema` for `{{catalog}}.{{schema}}` first.

## Two complementary signals

1. **`query_spill_v`** (Method A — SQL query history): per-statement spill
   (`spilled_local_bytes > 0`), last ~30 days, with compute type, job attribution, and a
   classified `root_cause` + prescriptive `root_cause_detail`. Best for SQL/warehouse work.
2. **`cluster_disk_pressure_v`** (Method B — node telemetry): cluster × day disk-free
   floor and swap. **Catches non-SQL Spark jobs** that never appear in query history.
3. **`expanded_disk_events`** (durable log): the *actual* elastic-disk expansions and
   failures from the Cluster Events API, joined to the job running at that moment.

Use `cluster_names` to resolve `cluster_id` → `cluster_name`.

## root_cause taxonomy

`query_spill_v.root_cause` ∈ {`WIDE_SHUFFLE`, `NO_PRUNING`, `MV_REFRESH`, `MEMORY_BOUND`,
`MODERATE`}; `pressure_signal` ∈ {`DISK_PRESSURE`, `MEM_SWAP`, `OK`}. Logic + fixes:
**`references/spill-taxonomy.md`**.

## Answer queries (open only the one you need)

| Question | Query file |
|---|---|
| What spilled the most, and why? | `queries/top_spilling_statements.sql` |
| Where is spill concentrated (cause × compute)? | `queries/spill_root_cause_rollup.sql` |
| Which clusters are low on disk / swapping? | `queries/cluster_disk_pressure.sql` |
| When did disks actually expand or fail to? | `queries/expanded_disk_events.sql` |

## Output contract

For remediation recommendations use **Problem / Action / $ Savings (usually none —
spill is a performance/stability fix) / Effort (S-M-L) / Risk (L-M-H) / Docs** — see
`references/output-contract.md`. `root_cause_detail` gives the concrete fix; translate
if needed and cite a real docs.databricks.com URL for the tuning action.

## Guardrails

- `SELECT` only; never DDL/DML. (Re)building views is the `job-monitoring-setup` skill's job.
- `statement_text` is truncated to ~400 chars — treat as a hint, not the full query.
- `expanded_disk_events` has no rows for serverless (no cluster events) and only retains
  what was snapshotted before the API purged terminated job clusters (~30 days).
