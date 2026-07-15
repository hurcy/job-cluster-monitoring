# End-to-End Scenario — Genie Code Compute-Optimization Co-Pilot

This document walks through, from start to finish, how an admin uses — in natural language, inside a Databricks
workspace — the **6 Genie Code Skills** registered by this deployment.
Every figure is a value obtained by deploying and running against the actual `e2-demo-field-eng` workspace
(`main.cinyoung_hur_monitoring`).

**Skills featured**: `job-cluster-monitoring` (router) · `job-monitoring-setup` · `compute-cost-analysis` ·
`compute-right-sizing` · `disk-spill-diagnosis` · `monitoring-data-quality`.

**Persona**: *Jisoo* — a platform admin. She's been asked to "find where Databricks compute cost has risen versus
last quarter and bring it down." She uses the workspace's **Genie Code** panel as her co-pilot.

> Assumption: the admin has deployed this repo with `make deploy TARGET=<ws>`, so the 6 skills are registered under
> `/Workspace/.assistant/skills/` (or a personal folder). All analysis skills are **read-only**.

---

## Act 0 — Setup: "Create the monitoring tables first"

Jisoo types into a new Genie Code conversation:

> **"Set up job and cluster monitoring for this workspace, so I can analyze right-sizing and spill."**

- **Skill loaded**: `job-cluster-monitoring` (router) → `job-monitoring-setup`.
- **Behind the scenes**: the skill checks prerequisites (`SELECT 1 FROM system.compute.node_timeline`,
  whether `system.compute.clusters` is provisioned), then guides and runs the `make deploy` → `make refresh` procedure.
  The `workload_monitoring_refresh` job **queries `system.*` directly on a classic cluster** to create
  9 tables/views in `catalog.schema`. Table/column comments are declared inline in the CREATE statements and are
  used directly as Genie metadata.
- **Result (measured)**: `instance_workload_analysis_mv` 318,001 rows · `job_run_cost_analysis_mv` 302,131 rows ·
  `right_sizing_analysis_mv` 3,585 rows · `query_spill_v` 18,013 rows · `cluster_disk_pressure_v` 100,802 rows.

> If any analysis skill hits "table not found," the router automatically routes it to this skill to prompt setup first.

---

## Act 1 — The big picture: "Where is the money going?"

> **"Show me the last 90 days of compute cost, split into classic and serverless. And the most expensive jobs, too."**

- **Skill loaded**: `compute-cost-analysis`.
- **Behind the scenes**: runs `queries/cost_by_compute_type.sql` and `queries/top_spenders.sql`
  against `job_run_cost_analysis_mv` (with `{{catalog}}.{{schema}}` substituted).
- **Genie response (measured)**:
  - Total compute **$191,077 / 90 days** — Classic **$141,394 (74%)**, Serverless **$49,683 (26%)**.
  - "Classic accounts for three-quarters. Classic has room to be resized, so let's start there."
- **Prompt for the next action**: "A large share of the Classic cost may be underutilized clusters —
  shall we move on to right-sizing?" → Jisoo: "Yes."

---

## Act 2 — The crux: "Find the oversized clusters and tell me the savings"

> **"Which clusters are over-provisioned? How much would I save by shrinking them?"**

- **Skill loaded**: `compute-right-sizing`.
- **Behind the scenes**: runs `queries/top_downsize_candidates.sql` (DEFINITE/LIKELY_DOWNSIZE ordered by savings)
  and `queries/savings_by_owner.sql` against `right_sizing_analysis_mv`. The `sizing_recommendation`
  taxonomy and thresholds are interpreted via `references/sizing-taxonomy.md`.
- **Genie response — output contract (Problem/Action/$Savings/Effort/Risk/Docs)** (measured):

  | # | Problem | Action | $ Savings(90d) | Effort | Risk |
  |---|---|---|---|---|---|
  | 1 | Harika's All-Purpose cluster `r5d.8xlarge × 27 workers` at **avg CPU 0.6%, avg memory 4.2%** (2 runs) — `DEFINITE_DOWNSIZE` | Cut worker count by 50% or use a smaller instance type | **$3,290** | S | L |
  | 2 | Rahul's `g5.12xlarge × 4` GPU cluster at CPU 0.8% — `DEFINITE_DOWNSIZE` | Reduce workers/type (reconsider whether GPU is needed) | **$1,476** | S | L |
  | 3 | `payments-gts-wib-rtm` `rd-fleet.4xlarge × 40` at CPU 2.4% — `DEFINITE_DOWNSIZE` | Reduce worker count | **$1,458** | M | M |

  - **Concentration by owner** (where to start): harika **$6,581** · ketan **$3,985** · brandon **$3,348** ·
    zach $1,909 · rahul $1,476 · ameer $1,113.
  - Each Action is backed by cluster sizing / autoscaling / policy documentation URLs on `docs.databricks.com`.
  - The Korean `recommendation_detail` is translated into the user's language and reflected in Problem/Action.
- **Guardrails**: even at low utilization, `INSUFFICIENT_RUNS`/`NO_UTIL_DATA` are marked "cannot determine"
  (to avoid over-recommending). Because All-Purpose clusters are shared by multiple jobs, it also warns
  "N jobs share this cluster."

Jisoo: "Just handling harika and ketan alone saves over $10K a quarter. But why is that payments job using something so big?"

---

## Act 3 — Root cause: "Why is this job so big and slow?"

> **"Tell me whether the payments-related jobs spill to disk, and if so, why."**

- **Skill loaded**: `disk-spill-diagnosis`.
- **Behind the scenes**: runs `queries/spill_root_cause_rollup.sql` and `queries/top_spilling_statements.sql`
  against `query_spill_v`. The `root_cause`/`pressure_signal` taxonomy is interpreted via `references/spill-taxonomy.md`.
- **Genie response (aggregated by root_cause — measured, `main.cinyoung_hur_monitoring`)**:
  - `MV_REFRESH` is the overwhelming #1 for spill: **~71.8 TB** (9,980 statements, 55 jobs) — suspected full recompute. Action: verify incremental refresh.
  - `WIDE_SHUFFLE`: **~26.9 TB** (488 statements, avg 55 GB per statement; SQL Warehouse averages **135 GB**) — Action: AQE skew join, raise shuffle partitions, Z-order on join/dedup keys.
  - `NO_PRUNING`: **~16.1 TB** (7,150 statements, 56 jobs) — full scans with zero file pruning. Action: add partition/clustering predicates.
  - `MEMORY_BOUND`: spill > input — Action: use memory-optimized nodes or reduce partitions.
- **Hand-off**: if spill is `MEMORY_BOUND` and the cluster has swap, it says "this isn't oversizing but
  **memory pressure** — in right-sizing, connect to a memory-optimized-type recommendation instead of a downsize," and
  cross-checks against `compute-right-sizing`'s `CONSIDER_UPSIZE` signal (swap/spill/P95).
- **Disk pressure**: `queries/cluster_disk_pressure.sql` also presents `DISK_PRESSURE` (local disk free <8GB) /
  `MEM_SWAP` clusters — capturing even non-SQL Spark jobs that SQL doesn't catch.

Jisoo: "So the MV refresh was recomputing everything each time. Switching to incremental should cut both spill and runtime."

---

## Act 4 — Trust: "Can I trust these numbers?"

> **"Before I send this up, verify the consistency of these aggregate numbers."**

- **Skill loaded**: `monitoring-data-quality`.
- **Behind the scenes**: runs `queries/run_all_tests.sql` (T01–T17) — checking whether the cost source
  reconciles with the sizing table, plus the serverless/classic split, UNION, duration rollup, and data quality.
- **Genie response (measured: 16/17 PASS)**:
  - ✅ Cost allocation (T01–T04): attributed classic $137,412 == sizing total, diff 0.
  - ✅ serverless/classic split (T05–T06), ✅ UNION consistency (T07–T09: 3,585 = 851+2,734),
    ✅ sanity (T13–T17: no negatives/NULLs/empty tables).
  - ❌ **T11** (job-compute duration): source 929,405 min vs rollup 845,008 min (~9%, exceeding the 5% tolerance).
    "The duration rollup is off by 9% — it doesn't affect the cost or sizing verdicts, but check the pipeline's
    duration aggregation." → the skill **catches the real discrepancy** and transparently reports the limits of trust.
- Jisoo cites the cost and savings figures with confidence, and reports only the duration metric with a caveat.

---

## Act 5 — Wrap-up: Act and iterate

In a single day, Jisoo:
1. Asked the owners of the top underutilized clusters (harika/ketan/…) to downsize → locked in **$10K+** in quarterly savings.
2. Requested switching the MV_REFRESH jobs to incremental → cutting spill and runtime together.
3. Since the `Workload Monitoring Refresh` job auto-refreshes **daily at 06:00**, next week she can re-query
   "clusters newly oversized versus last week" with the same skill.

> **Key point**: Jisoo never wrote a single line of SQL. The 6 skills encapsulate validated queries + decision
> taxonomy + output contract, so workspace users and admins can **optimize compute in natural language**.
> These skills' output contract (Problem/Action/$Savings/Effort/Risk/Docs) meshes with the evidence tier of
> `fe-cost-optimization-report`, so it can be dropped straight into the customer-facing cost-optimization briefings FE produces.

---

## Appendix — Quick question → skill routing table

| What the admin asks | Triggered skill |
|---|---|
| "Set it up / no tables / refresh it" | `job-monitoring-setup` |
| "How much cost / DBU / classic vs serverless / most expensive job" | `compute-cost-analysis` |
| "Oversized / how much if I shrink / autoscale / underutilized" | `compute-right-sizing` |
| "Why is it slow / spill / shuffle / disk full / EXPANDED_DISK" | `disk-spill-diagnosis` |
| "Can I trust this number / validate / reconcile" | `monitoring-data-quality` |
| Broad/ambiguous ("how's compute doing") | `job-cluster-monitoring` (router) dispatches |
