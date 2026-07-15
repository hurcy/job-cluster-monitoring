# Recommendation output contract (spill remediation)

Present each remediation as six fields (same shape as `compute-right-sizing`, so it
drops into `fe-cost-optimization-report`'s evidence layer):

| Field | Notes for spill |
|---|---|
| **Problem** | Measured spill/pressure with numbers — e.g. "job X: 380 GB spill, WIDE_SHUFFLE, 12 statements/day". |
| **Action** | The fix from `root_cause_detail` — repartition / add pruning filter / incremental MV refresh / memory-optimized node. |
| **$ Savings** | Usually **none** — spill remediation is a performance/stability fix. If it enables a downsize, hand off to `compute-right-sizing` for the savings figure. |
| **Effort** | S / M / L (query hint = S; data layout / clustering = M; pipeline redesign = L). |
| **Risk** | L / M / H. |
| **Docs** | A real `docs.databricks.com` URL for the tuning technique (spill/shuffle/pruning/liquid clustering/AQE). Do not invent — verify the current slug first. |

Translate Korean detail text into the user's language; never paste raw Korean into an
otherwise-non-Korean answer.
