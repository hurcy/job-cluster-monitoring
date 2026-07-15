# Recommendation output contract

Every right-sizing (or spill-remediation) recommendation is presented with these six
fields, so it drops cleanly into `fe-cost-optimization-report`'s evidence layer.

| Field | Source |
|---|---|
| **Problem** | The measured symptom, with numbers — e.g. "22% mean CPU, 18% mean mem over 42 runs, 0 spill". Pull from the utilization/spill columns. |
| **Action** | The concrete change — e.g. "downsize worker type Standard_D8ds_v5 → D4ds_v5" / "cap max_workers 8 → 4" / "enable Photon" / "switch to memory-optimized type" / "add a pruning predicate". |
| **$ Savings** | `estimated_savings_usd` (list-price estimate — say "estimated, list price", never present as a quote). Upsize/IO/burst rows have no savings. |
| **Effort** | S / M / L (S = config change; M = test + redeploy; L = code/pipeline change). |
| **Risk** | L / M / H (downsize on stable low use = L; shared All-Purpose cluster affecting many jobs = M/H; upsize = L). |
| **Docs** | A real `docs.databricks.com` URL that backs the Action. |

## Docs — cite, do not invent

Provide a **live, current** `docs.databricks.com` URL for each Action. Do not paste a
URL you are not sure resolves. If unsure, look it up (web/docs access) before quoting.
Canonical topics to cite (confirm the exact current slug first):

- Cluster/compute configuration best practices (sizing, worker types)
- Autoscaling behavior and when it helps vs hurts
- Photon (when it lowers cost by shortening runtime)
- Cluster policies (enforcing size ceilings / node types)
- Disk spill / memory tuning (for CONSIDER_UPSIZE driven by spill or swap)

## Translation

`recommendation_detail` / `root_cause_detail` are stored in Korean. Translate into the
user's language and fold into **Problem** and **Action**; never paste raw Korean into an
otherwise-non-Korean answer.
