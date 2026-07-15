# Job Cluster Monitoring

Deploys the Job & Cluster workload monitoring (**Right-Sizing** + **Disk-Spill**) dashboard via Databricks Asset Bundles (DAB).
**No Serverless / SDP (Lakeflow Declarative Pipeline)** — every analysis is produced by classic-cluster notebooks that query the `system.*` tables **directly**, so no copy of the system tables is required.

## Preview
![alt text](https://github.com/hurcy/job-cluster-monitoring/blob/main/dashboard_example.png)

## Genie Code Skills (co-pilot)

In addition to the dashboard, this repo ships its analysis queries and lifecycle as **Databricks Genie Code Skills**.
Registered as skills in [Genie Code](https://docs.databricks.com/aws/en/genie-code/skills) (the workspace-native AI assistant),
they let workspace users and admins optimize compute conversationally —
e.g., *"Which clusters are oversized and how much can I save?"*, *"What is spilling, and why?"*, *"What were this month's top-cost jobs?"*.

The 6 skills under `skills/` (`SKILL.md` = the same open Agent Skills standard as Claude Code):

| Skill | Role |
|------|------|
| `job-cluster-monitoring` | Umbrella router — table catalog, shared output contract, question routing |
| `job-monitoring-setup` | Pipeline deploy / refresh / UC comment re-seeding (the only writer) |
| `compute-right-sizing` | Over/under-provisioning diagnosis + estimated savings (`right_sizing_analysis_mv`, etc.) |
| `disk-spill-diagnosis` | Spill root cause / disk pressure / EXPANDED_DISK (`query_spill_v`, etc.) |
| `compute-cost-analysis` | DBU→USD, classic vs serverless, top spenders (`job_run_cost_analysis_mv`) |
| `monitoring-data-quality` | Aggregation reconciliation checks (T01–T17) |

- **End-to-end usage scenario**: [`SCENARIO.md`](SCENARIO.md) — a measurement-based walkthrough in which an admin runs
  setup → cost → right-sizing → spill → validation in natural language via Genie Code.
- **Analysis skills are read-only (SELECT)** — they only query the tables created by the setup skill.
- Recommendations follow the **Problem / Action / $ Savings / Effort / Risk / Docs** contract, meshing with
  the evidence tier of [`fe-cost-optimization-report`](https://github.com/databricks-field-eng/vibe).
  (That project mirrors the internal `centralized_system_tables`; this one reads the customer workspace's live `system.*` — the two are complementary.)

### Publish

```bash
# Deploy the pipeline (job + dashboard) AND publish skills to /Workspace/.assistant/skills
make deploy                                   # = bundle deploy + workspace import-dir skills

# Publish only the skills (to a personal folder)
make skills SKILLS_ROOT=/Users/<you>/.assistant/skills
```

> DAB has no skill resource type, so `bundle deploy` cannot place files at the `.assistant/skills` path
> (sync only covers the bundle root). Hence `databricks.yml` sets `sync.exclude: ["skills/**"]`,
> skills are published via `databricks workspace import-dir`, and `make deploy` ties the two together.
> After editing a skill in Genie Code, start a **new conversation thread** for the changes to take effect.

## Project structure

```
job-cluster-monitoring/
├── Makefile                        # make deploy (bundle + publish skills) / make refresh
├── databricks.yml                  # DAB bundle config (variables, targets, skills/ sync excluded)
├── build_dashboard.py              # Retarget the deployed dashboard (read via export → write via Lakeview PATCH)
├── skills/                         # Genie Code Skills (→ /Workspace/.assistant/skills)
│   ├── job-cluster-monitoring/     # Umbrella router + shared output contract
│   ├── job-monitoring-setup/       # Deploy / refresh / comment re-seeding (the only writer)
│   ├── compute-right-sizing/       # Over/under-provisioning diagnosis + savings (queries/ + references/)
│   ├── disk-spill-diagnosis/       # spill / disk pressure / EXPANDED_DISK (queries/ + references/)
│   ├── compute-cost-analysis/      # DBU→USD, classic vs serverless, top spenders (queries/)
│   └── monitoring-data-quality/    # reconciliation checks (T01–T17)
├── resources/
│   ├── jobs.yml                    # Job resources (setup: 2 tasks; comments inlined in CREATE)
│   └── dashboard.yml               # AI/BI dashboard resource
├── workload_analysis/
│   ├── ingestion/                  # Setup notebooks (classic cluster, direct system.* queries)
│   │   ├── workload_analysis_setup.py     # Create Right-Sizing tables (comments inlined/co-located)
│   │   └── spill_audit_setup.py           # Disk-Spill views/tables + EXPANDED_DISK (comments inlined)
│   ├── explorations/               # Exploration notebooks
│   └── validation/                 # Data-quality validation queries
└── dashboard/
    └── *.lvdash.json               # Dashboard definition (Right-Sizing + Disk Spill pages)
```

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.218+
- An authentication profile configured for the target workspace (`~/.databrickscfg`)
- Access to Unity Catalog and the system tables

## Deployment

### 1. Configure the authentication profile

Add the target workspace profile to `~/.databrickscfg`.

```ini
[DEFAULT]
host  = https://adb-984752964297111.11.azuredatabricks.net
token = dapi...

[WORKSPACE_B]
host  = https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net
token = dapi...
```

### 2. Review the bundle variables

Adjust the variables defined in `databricks.yml` to match the target workspace.

| Variable | Description | Example |
|------|------|------|
| `catalog` | Unity Catalog catalog name (where analysis results are written) | `hurcy` |
| `analytics_schema` | Output schema for analysis results (Spill views + Right-Sizing tables) | `test` |
| `workspace_id` | ID of the workspace being analyzed | `984752964297111` |
| `warehouse_id` | SQL Warehouse for the dashboard | `Shared Endpoint` (lookup) |

### 3. Add a new target (deploy to another workspace)

Add a new target to the `targets` section of `databricks.yml`.

```yaml
targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://adb-984752964297111.11.azuredatabricks.net
      profile: DEFAULT

  # Example: another workspace
  prod:
    workspace:
      host: https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net
      profile: WORKSPACE_B
    variables:
      catalog: "my_catalog"
      analytics_schema: "my_schema"
      workspace_id: "xxxxxxxxxxxx"
      warehouse_id: "xxxxxxxxxxxxxxxx"
```

### 4. Run the deployment

```bash
# Deploy the default target (dev)
databricks bundle deploy

# Deploy a specific target
databricks bundle deploy -t prod

# Override variables
databricks bundle deploy -t prod \
  --var="catalog:other_catalog" \
  --var="workspace_id:123456789"
```

> **When deploying to a different catalog.schema** — after deploying with the `analytics_schema`/`catalog` variables changed,
> retarget **the deployed dashboard** with `python3 build_dashboard.py` (see step 6 below).

### 5. Run the analysis job (no Serverless / SDP)

```bash
# Create/refresh the Right-Sizing tables + Disk-Spill views/events in one job (two tasks)
# — classic cluster queries system.* directly
databricks bundle run workload_monitoring_refresh -t <target>
```

> **Tuning the Right-Sizing thresholds:** the sizing-decision thresholds (min_runs, downsize/likely CPU·Mem,
> upsize P95, swap, spill, iowait) are exposed as **notebook parameters** of the `workload_analysis_setup` task.
> Defaults live in `base_parameters` in `resources/jobs.yml`; adjust them at runtime via the Jobs UI
> *Run now with different parameters*.

> **Right-Sizing prerequisite:** `system.compute.clusters` must be queryable. On some workspaces it may be
> unprovisioned (`UC_DEPENDENCY_DOES_NOT_EXIST`), so verify beforehand. Disk-Spill uses only
> `system.query.history` / `system.compute.node_timeline` and has no such constraint.

### 6. Retarget the dashboard (`build_dashboard.py`)

Through Global Filters, the dashboard lets you change the **catalog**, **schema**, and **lookback window** at runtime.
Adding the Disk-Spill page and retargeting catalog/schema work by **directly modifying the deployed dashboard** —
run this **after** `databricks bundle deploy`:

```bash
# Read the deployed dashboard via the Workspace Export API → retarget → write it back via the Lakeview PATCH API
python3 build_dashboard.py
```

- catalog/schema are read from `databricks.yml` (override with `CATALOG=`/`SCHEMA=`).
- The target dashboard is located by the display name `[<target>] Job Cluster Monitoring Dashboard`
  (override with `DASHBOARD_ID`/`DASHBOARD_NAME`/`TARGET`; authentication is SDK-standard — the bundle target profile).
- **It does not modify the local `.lvdash.json`.** (idempotent — safe to run multiple times)

### 7. Check deployment status / destroy

```bash
# Check deployment status
databricks bundle validate -t prod

# Destroy resources
databricks bundle destroy -t prod
```

## Disk-Spill / Expanded-Disk audit

Analyzes the **local disk spill** and **elastic-disk expansion pressure** of code/queries run on
Job Compute, Serverless, and SQL Warehouses, and surfaces them on the dashboard's **Disk Spill** /
**Spill Detail & Expanded Disk** pages. (Source: `spill_audit_notebook.py`)

Tailored to **Pro SQL Warehouse environments that cannot use Serverless**, the spill analysis layer is implemented
as **plain SQL VIEWs** rather than DLT MVs — because a Pro warehouse queries the system tables directly,
no serverless, DLT, or copy of the system tables is needed at all. **The analysis window is the last 30 days.**

| Signal | Source (direct query) | Output | Runtime |
|------|------|--------|------|
| Method A — statement-level spill | `system.query.history` | `query_spill_v` (compute type · job attribution · root-cause tag) | Pro SQL Warehouse |
| Method B — cluster disk-free floor / swap | `system.compute.node_timeline` | `cluster_disk_pressure_v` (expansion signal, incl. non-SQL jobs) | Pro SQL Warehouse |
| Actual expansion events | Cluster Events API (`/api/2.0/clusters/events`) | `expanded_disk_events` Delta table (durable log) | Classic single-node cluster |

**Root-cause tags** (`query_spill_v.root_cause`): `WIDE_SHUFFLE` · `NO_PRUNING` ·
`MV_REFRESH` · `MEMORY_BOUND` · `MODERATE` — each carries its prescription in `root_cause_detail`.

### Variable-driven deployment (arbitrary catalog.schema / workspace)

The views, tables, and event ingestion are all created by a single `spill_audit_setup.py` notebook, fully
parameterized by **bundle variables** (since `CREATE VIEW` cannot take parameter markers, they are injected via
Python f-strings). **No per-customer SQL changes required.** Per-customer deployment steps:

1. Set the `databricks.yml` variables: `catalog`, `analytics_schema`, `workspace_id`, `warehouse_id`.
2. `databricks bundle deploy -t <target>` — deploy the dashboard, views, tables, and job (the dashboard is created from the local `.lvdash.json`).
3. `python3 build_dashboard.py` — read the **deployed** dashboard via the Workspace Export API, add the Disk-Spill page, bulk-replace the `p_catalog`/`p_schema` defaults, global filters, and driver dataset with your catalog/schema, then **apply via the Lakeview PATCH API**. (overridable with `CATALOG=`/`SCHEMA=`/`DASHBOARD_ID`/`TARGET`; idempotent)
4. `databricks bundle run workload_monitoring_refresh -t <target>` — run the analysis job.

> **Prerequisites (customer workspace):** the `query`/`compute`/`lakeflow` **system schemas enabled**,
> one Pro/Serverless **SQL Warehouse**, plus `CREATE` on the target schema and `SELECT` on the system tables.
> Because the system tables are multi-workspace, a `workspace_id` filter is mandatory.

> **Note on EXPANDED_DISK events:** actual expansion events live only in the Cluster Events API, not in a system
> table (= not callable from SQL; a notebook + compute is required), and **terminated job clusters are purged after
> ~30 days**, after which they cannot be queried. Serverless has no such events. That is why the notebook snapshots
> the events to Delta **daily**, building a **durable event log** that outlives the API retention window — it may be
> empty initially and fills in over time. The immediate signal for every cluster is covered by
> `cluster_disk_pressure_v` (disk-free floor).

The `spill_audit_setup` task of the merged **`Workload Monitoring Refresh`** job (`resources/jobs.yml`) runs
daily — the `spill_audit_setup` notebook creates the views/tables and then ingests the events on a **classic
single-node cluster** (no serverless). (The same job's `workload_analysis_setup` task creates the Right-Sizing tables.)

## License
MIT License
