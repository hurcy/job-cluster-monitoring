# root_cause & pressure_signal taxonomy

## `query_spill_v.root_cause` (per statement)

Classified by heuristics over `system.query.history` metrics. `root_cause_detail`
carries the concrete tuning guidance (English).

| root_cause | Heuristic (approx) | What it means | Typical fix |
|---|---|---|---|
| `WIDE_SHUFFLE` | `shuffle_gb > 1.2 × read_gb` **and** `spill_gb > 0.5 × shuffle_gb` | Shuffle far exceeds input and drives the spill | Reduce shuffle: better join order / broadcast small side, repartition, enable AQE, pre-aggregate |
| `NO_PRUNING` | `read_files ≥ 1000` **and** `pruned_files = 0` | Full scan — no file/partition pruning | Add selective predicates on partition/clustering keys; Z-order / liquid clustering; check stats |
| `MV_REFRESH` | `statement_type = 'REFRESH'` | Materialized-view / streaming-table refresh spilling | Incremental refresh, partition the MV, right-size the refresh compute |
| `MEMORY_BOUND` | `spill_gb > read_gb` | Working set exceeds memory | Memory-optimized node type / more RAM; reduce partition size; split the job |
| `MODERATE` | none of the above, but spill > 0 | Minor spill | Usually tolerable; revisit only if `exec_s` is high |

Columns to cite: `spill_gb`, `read_gb`, `shuffle_gb`, `read_files`, `pruned_files`,
`exec_s`, `statement_type`, `compute_label`.

## `cluster_disk_pressure_v.pressure_signal` (per cluster × day)

| pressure_signal | Trigger | Meaning |
|---|---|---|
| `DISK_PRESSURE` | `min_disk_free_gb < 8` GB | Local disk nearly full — spill/expansion risk (even for non-SQL Spark) |
| `MEM_SWAP` | `max_swap_pct > 0` | Host swapped — RAM exhausted; strong CONSIDER_UPSIZE signal |
| `OK` | otherwise | No pressure that day |

`mins_disk_low` = minutes under the 8 GB floor that day (severity). Cross-reference a
`DISK_PRESSURE` cluster with `expanded_disk_events`: a `DID_NOT_EXPAND_DISK` with a
populated `cause` on the same cluster confirms real disk starvation.

## Handoff to right-sizing

High `spill_gb` (> 50 GB / 90d) or `MEM_SWAP` feeds the `CONSIDER_UPSIZE` signal in the
`compute-right-sizing` skill — if a spill is `MEMORY_BOUND` and the cluster also shows
swap, recommend a memory-optimized/larger node there rather than only a query fix.
