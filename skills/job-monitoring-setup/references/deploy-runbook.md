# Deploy & refresh runbook

## Bundle variables (`databricks.yml`)

| Var | Default | Purpose |
|---|---|---|
| `catalog` | `hurcy` | Output catalog for all tables/views |
| `analytics_schema` | `test` | Output schema |
| `workspace_id` | `984752964297111` | Scopes every `system.*` read to this workspace |
| `warehouse_id` | lookup `Shared Endpoint` | Warehouse for the dashboard |

Override per target or with `--var`, e.g.:
`databricks bundle deploy --var catalog=acme --var analytics_schema=monitoring --var workspace_id=123...`

## Make targets

| Command | Does |
|---|---|
| `make deploy` | `bundle deploy` (jobs + dashboard) **then** `import-dir skills → SKILLS_ROOT` |
| `make bundle` | Just the DAB deploy (jobs + dashboard) |
| `make skills` | Just publish the Genie Code skills to `SKILLS_ROOT` |
| `make refresh` | `databricks bundle run workload_monitoring_refresh` |

`SKILLS_ROOT` (Makefile var):
- **Workspace-wide** (all users, needs admin): `/Workspace/.assistant/skills` (default).
- **Personal** (just you): `/Users/<you>/.assistant/skills`.

> Why `import-dir` and not pure DAB? DAB has no "skill" resource type and can't place
> files at an arbitrary `/Workspace/.assistant/skills` path — its sync always targets the
> bundle root. Databricks' own `genie-code-skills-demo` also publishes skills with
> `workspace import` / `import-dir`. So `databricks.yml` has `sync.exclude: ["skills/**"]`
> (skills are published out-of-band) and `make deploy` wraps both steps into one command.
> If a workspace rejects `.md` via `import-dir`, fall back to per-file
> `databricks workspace import ... --format RAW --overwrite`.

## Job structure (`resources/jobs.yml` → `workload_monitoring_refresh`)

Daily `0 0 6 * * ?` Asia/Seoul. Two job clusters, two parallel tasks:

1. **`workload_analysis_setup`** (notebook, autoscaling Photon `analysis_cluster`) —
   builds the right-sizing tables. Carries the tunable thresholds below.
2. **`spill_audit_setup`** (notebook, single-node `spill_setup_cluster`) — builds the
   disk-spill views + `expanded_disk_events`. Needs a *live* cluster because the Cluster
   Events API only returns events for running/recent clusters.

Table/column comments are declared inline in each notebook's CREATE TABLE/VIEW statements
(no separate comment task).

## Tunable right-sizing thresholds (`workload_analysis_setup.base_parameters`)

| Param | Default | Signal |
|---|---|---|
| `min_runs` | 3 | Min runs in 90d before judging sizing (gates DOWNSIZE/UPSIZE/BURST) |
| `downsize_cpu_avg` | 20 | DEFINITE_DOWNSIZE: avg CPU% below |
| `downsize_mem_avg` | 30 | DEFINITE_DOWNSIZE: avg Mem% below |
| `likely_cpu_avg` | 30 | LIKELY_DOWNSIZE: avg CPU% below |
| `likely_mem_avg` | 40 | LIKELY_DOWNSIZE: avg Mem% below |
| `upsize_mem_p95` | 85 | CONSIDER_UPSIZE: Mem P95% above |
| `upsize_cpu_p95` | 85 | CONSIDER_UPSIZE: CPU P95% above |
| `swap_pct` | 0 | CONSIDER_UPSIZE: max swap% above |
| `spill_gb` | 50 | CONSIDER_UPSIZE: 90d disk spill GB above |
| `iowait_pct` | 10 | IO_BOTTLENECK: avg CPU iowait% above |

Disk-spill task (`spill_audit_setup.base_parameters`): `lookback_days` (30),
`max_clusters` (150, candidate clusters queried against the Cluster Events API).

## Troubleshooting

- **`TABLE_OR_VIEW_NOT_FOUND`** in an analysis skill → tables not built yet; run `make refresh`.
- **Right-sizing tables empty / error** → `system.compute.clusters` not provisioned; enable
  the compute system schema. Disk-spill still works without it.
- **No `expanded_disk_events` rows** → normal for serverless-only or if clusters terminated
  and were purged by the API (~30 days) before the first snapshot; the daily job accrues them.
- **Comments missing after refresh** → a setup notebook failed before its inline comment
  statements ran; check the task logs and re-run `make refresh`.
- **Legacy docs** in `doc/*.md` describe a decommissioned serverless-SDP design with
  different object names — ignore for object names; use only for design rationale.
