# Databricks notebook source
# MAGIC %md
# MAGIC # build_dashboard — target the Job Cluster Monitoring dashboard to a catalog.schema
# MAGIC
# MAGIC Adds the Disk Spill pages + datasets and (re)targets the WHOLE dashboard (every
# MAGIC `p_catalog`/`p_schema` parameter default, the catalog/schema global filters, and the
# MAGIC driver dataset) to a given `catalog.schema`. Idempotent.
# MAGIC
# MAGIC **Dual-mode — same file runs in two places:**
# MAGIC - **In the Databricks workspace** (this notebook): set the widgets `catalog`, `schema`,
# MAGIC   `dashboard_id` (from the dashboard URL `.../dashboardsv3/<id>/...`), and optional
# MAGIC   `publish_warehouse_id`, then **Run All**. It fetches the deployed Lakeview dashboard via
# MAGIC   the API, retargets it, **updates** it, and **publishes**.
# MAGIC - **Locally** (`python3 build_dashboard.py`): reads `catalog`/`analytics_schema` from
# MAGIC   `databricks.yml` and rewrites `dashboard/Job Cluster Monitoring Dashboard.lvdash.json`
# MAGIC   in place (for `databricks bundle deploy`). `CATALOG=…/SCHEMA=…/OUT=…` env vars override.

# COMMAND ----------

import json, os, re


def transform(d, catalog, schema):
    """Add Disk Spill pages/datasets and (re)target the whole dashboard to catalog.schema.
    Idempotent: removes any previously-added spill datasets/pages first. Returns the dict."""
    SPILL_DATASETS = {"ds-spill", "ds-spill-jobs", "ds-disk-pressure", "ds-expanded-disk"}
    SPILL_PAGES = {"page-spill", "page-spill-detail"}
    d["datasets"] = [ds for ds in d["datasets"] if ds["name"] not in SPILL_DATASETS]
    d["pages"] = [p for p in d["pages"] if p["name"] not in SPILL_PAGES]

    # ---------- helpers ----------
    def params():
        def p(disp, kw, val):
            return {"displayName": disp, "keyword": kw, "dataType": "STRING",
                    "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": val}]}}}
        return [p("카탈로그", "p_catalog", catalog), p("스키마", "p_schema", schema)]

    def dataset(name, disp, sql):
        return {"name": name, "displayName": disp,
                "queryLines": [l + "\n" for l in sql.strip("\n").split("\n")],
                "parameters": params()}

    def ident(mv):
        return f"IDENTIFIER(:p_catalog || '.' || :p_schema || '.{mv}')"

    def text(name, lines, x, y, w, h):
        return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
                "position": {"x": x, "y": y, "width": w, "height": h}}

    def counter(name, ds, fname, expr, title, x, y, w=2, h=3):
        return {"widget": {"name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": ds,
                "fields": [{"name": fname, "expression": expr}], "disaggregated": False}}],
            "spec": {"version": 2, "widgetType": "counter",
                "encodings": {"value": {"fieldName": fname, "displayName": title}},
                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

    def bar(name, ds, dim, dim_disp, measure_name, measure_expr, measure_disp, title, x, y, w, h, colors=None):
        return {"widget": {"name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": ds,
                "fields": [{"name": dim, "expression": f"`{dim}`"},
                           {"name": measure_name, "expression": measure_expr}],
                "disaggregated": False}}],
            "spec": {"version": 3, "frame": {"showTitle": True, "title": title},
                "mark": {"colors": colors or ["#00A972", "#FFAB00", "#FF3621", "#8BCAE7", "#AB4ABA", "#FCA4A1"]},
                "widgetType": "bar",
                "encodings": {
                    "x": {"fieldName": dim, "displayName": dim_disp, "scale": {"type": "categorical"}},
                    "y": {"fieldName": measure_name, "displayName": measure_disp, "scale": {"type": "quantitative"}},
                    "label": {"show": True}}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

    def tbl(name, ds, cols, title, x, y, w, h):
        fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in cols]
        enc = [{"fieldName": c[0], "displayName": c[1]} for c in cols]
        return {"widget": {"name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": True}}],
            "spec": {"version": 2, "frame": {"showTitle": True, "title": title},
                "widgetType": "table", "encodings": {"columns": enc}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

    # ---------- datasets ----------
    d["datasets"].append(dataset("ds-spill", "Spill · per-statement", f"""
SELECT
  spill_date AS run_start_time,
  compute_label, is_job_attributed, job_id, job_name, warehouse_id, cluster_id,
  statement_type, executed_by,
  spill_gb, read_gb, shuffle_gb, read_files, pruned_files, exec_s,
  root_cause, root_cause_detail, statement_text, statement_id
FROM {ident('query_spill_v')}
ORDER BY spill_gb DESC
"""))

    d["datasets"].append(dataset("ds-spill-jobs", "Spill · job-attributed only", f"""
SELECT
  spill_date AS run_start_time,
  job_id, job_name, compute_label, spill_gb, shuffle_gb, exec_s, root_cause
FROM {ident('query_spill_v')}
WHERE job_id IS NOT NULL
"""))

    d["datasets"].append(dataset("ds-disk-pressure", "Cluster disk-free / swap pressure", f"""
SELECT
  sample_date AS run_start_time,
  cluster_id, samples, avg_mem_pct, max_swap_pct, min_disk_free_gb, mins_disk_low, pressure_signal
FROM {ident('cluster_disk_pressure_v')}
ORDER BY min_disk_free_gb ASC
"""))

    d["datasets"].append(dataset("ds-expanded-disk", "Actual EXPANDED_DISK events", f"""
SELECT
  event_date AS run_start_time,
  event_time, cluster_id, event_type, instance_id, previous_disk_size, disk_size, free_space_bytes, cause
FROM {ident('expanded_disk_events')}
ORDER BY event_time DESC
"""))

    # ---------- page 1: Disk Spill overview ----------
    page1 = {"name": "page-spill", "displayName": "Disk Spill", "pageType": "PAGE_TYPE_CANVAS", "layout": [
        text("spill-title", ["## 💽 Disk Spill Audit — Job Compute · Serverless · SQL Warehouse"], 0, 0, 6, 1),
        text("spill-subtitle", [
            "로컬 디스크로 spill 한 코드/쿼리의 현황. `system.query.history` 기반 (Method A). "
            "Compute Type 으로 **SQL Warehouse / Serverless / Classic** 을, Root Cause 로 튜닝 방향을 구분한다. "
            "_기간/카탈로그/스키마는 Global Filters 페이지에서 조정._"], 0, 1, 6, 2),
        counter("spill-kpi-total", "ds-spill", "spill_gb_total", "SUM(`spill_gb`)", "Total Spill (GB)", 0, 3),
        counter("spill-kpi-stmts", "ds-spill", "stmt_count", "COUNT(`statement_id`)", "Spilling Statements", 2, 3),
        counter("spill-kpi-jobs", "ds-spill-jobs", "job_count", "COUNT(DISTINCT `job_id`)", "Spilling Jobs", 4, 3),
        text("spill-sec-1", ["### Spill by source & root cause"], 0, 6, 6, 1),
        bar("spill-by-compute", "ds-spill", "compute_label", "Compute Type",
            "sum(spill_gb)", "SUM(`spill_gb`)", "Spill (GB)", "Spill GB by Compute Type", 0, 7, 3, 6),
        bar("spill-by-cause", "ds-spill", "root_cause", "Root Cause",
            "sum(spill_gb)", "SUM(`spill_gb`)", "Spill (GB)", "Spill GB by Root Cause", 3, 7, 3, 6),
        text("spill-sec-2", ["### Top spilling jobs (Job Compute)"], 0, 13, 6, 1),
        bar("spill-top-jobs", "ds-spill-jobs", "job_name", "Job",
            "sum(spill_gb)", "SUM(`spill_gb`)", "Spill (GB)", "Top Jobs by Total Spill (GB)", 0, 14, 6, 7),
    ]}

    # ---------- page 2: Spill Detail & Expanded Disk ----------
    page2 = {"name": "page-spill-detail", "displayName": "Spill Detail & Expanded Disk", "pageType": "PAGE_TYPE_CANVAS", "layout": [
        text("spilld-title", ["## 🔎 Spill Detail · Disk Pressure · Expanded-Disk Events"], 0, 0, 6, 1),
        text("spilld-subtitle", [
            "문장 단위 spill 진단(원인+처방), 클러스터 disk-free floor(elastic-disk 확장 신호), "
            "그리고 Cluster Events API 에서 적재한 **실제 EXPANDED_DISK 이벤트**."], 0, 1, 6, 1),
        tbl("spilld-detail", "ds-spill", [
            ("run_start_time", "Date"), ("compute_label", "Compute"), ("job_name", "Job"),
            ("warehouse_id", "Warehouse"), ("spill_gb", "Spill (GB)"), ("read_gb", "Read (GB)"),
            ("shuffle_gb", "Shuffle (GB)"), ("read_files", "Files"), ("pruned_files", "Pruned"),
            ("exec_s", "Exec (s)"), ("root_cause", "Root Cause"), ("root_cause_detail", "Diagnosis & Fix"),
            ("statement_text", "Statement")],
            "Per-statement Spill Diagnostics", 0, 2, 6, 10),
        text("spilld-sec-1", ["### Cluster disk-free / swap pressure (elastic-disk expansion signal)"], 0, 12, 6, 1),
        bar("spilld-pressure-dist", "ds-disk-pressure", "pressure_signal", "Pressure Signal",
            "count_distinct_cluster", "COUNT(DISTINCT `cluster_id`)", "Clusters", "Clusters by Pressure Signal", 0, 13, 2, 6),
        tbl("spilld-pressure", "ds-disk-pressure", [
            ("cluster_id", "Cluster"), ("run_start_time", "Date"), ("min_disk_free_gb", "Min Disk Free (GB)"),
            ("avg_mem_pct", "Avg Mem %"), ("max_swap_pct", "Max Swap %"), ("mins_disk_low", "Mins Disk<8GB"),
            ("pressure_signal", "Signal")],
            "Cluster Disk Pressure (worst floors first)", 2, 13, 4, 6),
        text("spilld-sec-2", [
            "### Actual EXPANDED_DISK events (durable log)",
            "_Cluster Events API 기반. Serverless 는 이벤트가 없고, 종료된 job 클러스터는 ~30일 후 purge 되므로 "
            "초기에는 비어 있을 수 있다 — 매일 적재되며 시간이 지날수록 채워진다._"], 0, 19, 6, 2),
        tbl("spilld-events", "ds-expanded-disk", [
            ("event_time", "Event Time"), ("cluster_id", "Cluster"), ("event_type", "Event"),
            ("instance_id", "Instance"), ("previous_disk_size", "Prev Disk"), ("disk_size", "New Disk"),
            ("free_space_bytes", "Free Bytes"), ("cause", "Cause")],
            "EXPANDED_DISK / DID_NOT_EXPAND_DISK Events", 0, 21, 6, 6),
    ]}

    d["pages"].append(page1)
    d["pages"].append(page2)

    # ---------- wire new datasets into the global date-range filter ----------
    for p in d["pages"]:
        if p["name"] == "page-global-filters":
            for w in p["layout"]:
                if w["widget"]["name"] == "global-filter-date-range":
                    q = w["widget"]["queries"]
                    enc = w["widget"]["spec"]["encodings"]["fields"]
                    q[:] = [x for x in q if not x["name"].startswith("spill-filter_")]
                    enc[:] = [x for x in enc if not x["queryName"].startswith("spill-filter_")]
                    for ds in ["ds-spill", "ds-disk-pressure", "ds-expanded-disk"]:
                        qn = f"spill-filter_{ds}_run_start_time"
                        q.append({"name": qn, "query": {"datasetName": ds, "fields": [
                            {"name": "run_start_time", "expression": "`run_start_time`"},
                            {"name": "run_start_time_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}],
                            "disaggregated": False}})
                        enc.append({"fieldName": "run_start_time", "queryName": qn})

    # ---------- retarget the WHOLE dashboard to catalog.schema ----------
    def set_param_default(ds, kw, val):
        for pr in ds.get("parameters", []):
            if pr.get("keyword") == kw:
                pr["defaultSelection"]["values"]["values"] = [{"value": val}]

    for ds in d["datasets"]:
        if ds["name"] == "44015feb":               # the catalog/schema driver dataset
            ds["queryLines"] = [f"select '{catalog}' as catalog, '{schema}' as schema"]
        set_param_default(ds, "p_catalog", catalog)
        set_param_default(ds, "p_schema", schema)

    for p in d["pages"]:
        if p["name"] == "page-global-filters":
            for w in p["layout"]:
                spec = w["widget"].get("spec", {})
                if spec.get("widgetType") == "filter-single-select":
                    flds = spec.get("encodings", {}).get("fields", [])
                    fname = flds[0].get("fieldName") if flds else ""
                    sel = spec.get("selection", {}).get("defaultSelection", {}).get("values")
                    if sel is not None and fname in ("catalog", "schema"):
                        sel["values"] = [{"value": catalog if fname == "catalog" else schema}]

    return d

# COMMAND ----------


def _in_databricks():
    try:
        dbutils  # noqa: F821  (injected in Databricks notebooks)
        return True
    except NameError:
        return False


if _in_databricks():
    # ===== WORKSPACE MODE: widgets → Lakeview API (fetch → retarget → update → publish) =====
    dbutils.widgets.text("catalog", "hurcy", "Target catalog")
    dbutils.widgets.text("schema", "test", "Target schema (analytics)")
    dbutils.widgets.text("dashboard_id", "", "Lakeview dashboard id (from the dashboard URL)")
    dbutils.widgets.text("publish_warehouse_id", "", "Warehouse id to publish (blank = keep current)")

    CATALOG = dbutils.widgets.get("catalog").strip()
    SCHEMA = dbutils.widgets.get("schema").strip()
    DID = dbutils.widgets.get("dashboard_id").strip()
    PUB = dbutils.widgets.get("publish_warehouse_id").strip()
    assert DID, "Set the 'dashboard_id' widget (the id in the dashboard URL .../dashboardsv3/<id>/...)."

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    cur = w.api_client.do("GET", f"/api/2.0/lakeview/dashboards/{DID}")
    d = transform(json.loads(cur["serialized_dashboard"]), CATALOG, SCHEMA)

    body = {"display_name": cur["display_name"],
            "serialized_dashboard": json.dumps(d, ensure_ascii=False),
            "warehouse_id": cur.get("warehouse_id", "")}
    if cur.get("etag"):
        body["etag"] = cur["etag"]
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{DID}", body=body)

    wh = PUB or cur.get("warehouse_id", "")
    if wh:
        w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{DID}/published",
                        body={"embed_credentials": True, "warehouse_id": wh})
        print(f"updated + published dashboard {DID} → {CATALOG}.{SCHEMA}")
    else:
        print(f"updated dashboard {DID} → {CATALOG}.{SCHEMA} (no warehouse set, skipped publish)")

else:
    # ===== LOCAL MODE: read databricks.yml + .lvdash.json file → write file (for bundle deploy) =====
    HERE = os.path.dirname(os.path.abspath(__file__))   # repo root — this script lives here
    DASH = os.path.join(HERE, "dashboard", "Job Cluster Monitoring Dashboard.lvdash.json")
    YML = os.path.join(HERE, "databricks.yml")
    OUT = os.environ.get("OUT", DASH)

    def bundle_var(name, fallback):
        """Read variables.<name>.default from databricks.yml (PyYAML if available, else regex)."""
        try:
            import yaml
            v = yaml.safe_load(open(YML)).get("variables", {}).get(name) or {}
            return v.get("default", fallback)
        except Exception:
            t = open(YML).read()
            m = re.search(rf'^  {re.escape(name)}:[^\n]*\n(?:^    [^\n]*\n)*?^    default:\s*"?([^"\n]+?)"?\s*$',
                          t, re.M)
            return m.group(1) if m else fallback

    CATALOG = os.environ.get("CATALOG") or bundle_var("catalog", "hurcy")
    SCHEMA = os.environ.get("SCHEMA") or bundle_var("analytics_schema", "test")
    print(f"target catalog.schema = {CATALOG}.{SCHEMA}  "
          f"(source: {'env override' if os.environ.get('CATALOG') else 'databricks.yml'})")

    d = transform(json.load(open(DASH)), CATALOG, SCHEMA)
    json.dump(d, open(OUT, "w"), indent=2, ensure_ascii=False)
    print(f"OK target={CATALOG}.{SCHEMA}  datasets={len(d['datasets'])}  pages={len(d['pages'])}  -> {OUT}")
