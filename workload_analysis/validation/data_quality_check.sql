-- =====================================================================
-- Validation: data quality (NULL / negative / empty data)
-- =====================================================================
-- Verify that in all MVs there are no negative values in the total_dbus,
-- total_cost_usd, and duration-related columns, and no NULLs in key columns.
--
-- Parameters:
--   ${source_catalog}   - pipeline target catalog
--   ${analytics_schema} - pipeline target schema
-- =====================================================================


-- =============================================================
-- T13: job_run_cost_analysis_mv negative cost check
-- =============================================================
SELECT
  'T13_job_run_negative_cost' AS test_name,
  SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END)     AS negative_dbus_rows,
  SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END) AS negative_cost_rows,
  CASE
    WHEN SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv;


-- =============================================================
-- T14: all_purpose_cluster_sizing_mv negative cost check
-- =============================================================
SELECT
  'T14_all_purpose_negative_cost' AS test_name,
  SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END)     AS negative_dbus_rows,
  SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END) AS negative_cost_rows,
  CASE
    WHEN SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv;


-- =============================================================
-- T15: job_compute_sizing_mv negative cost check
-- =============================================================
SELECT
  'T15_job_compute_negative_cost' AS test_name,
  SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END)     AS negative_dbus_rows,
  SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END) AS negative_cost_rows,
  CASE
    WHEN SUM(CASE WHEN total_dbus < 0 THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN total_cost_usd < 0 THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv;


-- =============================================================
-- T16: job_run_cost_analysis_mv NULL key column check
-- =============================================================
-- Check only classic compute rows (serverless may have NULL cluster_id)
SELECT
  'T16_job_run_null_keys' AS test_name,
  SUM(CASE WHEN workspace_id IS NULL THEN 1 ELSE 0 END) AS null_workspace,
  SUM(CASE WHEN cluster_id IS NULL THEN 1 ELSE 0 END)   AS null_cluster,
  SUM(CASE WHEN job_id IS NULL THEN 1 ELSE 0 END)       AS null_job,
  SUM(CASE WHEN job_run_id IS NULL THEN 1 ELSE 0 END)   AS null_job_run,
  SUM(CASE WHEN total_dbus IS NULL THEN 1 ELSE 0 END)   AS null_dbus,
  SUM(CASE WHEN total_cost_usd IS NULL THEN 1 ELSE 0 END) AS null_cost,
  CASE
    WHEN SUM(CASE WHEN workspace_id IS NULL THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN cluster_id IS NULL THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN job_id IS NULL THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN job_run_id IS NULL THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN total_dbus IS NULL THEN 1 ELSE 0 END) = 0
     AND SUM(CASE WHEN total_cost_usd IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv
WHERE is_serverless = false;


-- =============================================================
-- T17: negative duration check (all MVs)
-- =============================================================
SELECT
  'T17_negative_duration' AS test_name,
  (SELECT SUM(CASE WHEN run_duration_minutes < 0 THEN 1 ELSE 0 END)
   FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv) AS job_run_neg_dur,
  (SELECT SUM(CASE WHEN avg_run_duration_minutes < 0 THEN 1 ELSE 0 END)
   FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv) AS ap_neg_dur,
  (SELECT SUM(CASE WHEN avg_run_duration_minutes < 0 THEN 1 ELSE 0 END)
   FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv) AS jc_neg_dur,
  CASE
    WHEN COALESCE((SELECT SUM(CASE WHEN run_duration_minutes < 0 THEN 1 ELSE 0 END)
                   FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv), 0) = 0
     AND COALESCE((SELECT SUM(CASE WHEN avg_run_duration_minutes < 0 THEN 1 ELSE 0 END)
                   FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv), 0) = 0
     AND COALESCE((SELECT SUM(CASE WHEN avg_run_duration_minutes < 0 THEN 1 ELSE 0 END)
                   FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv), 0) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result;


-- =============================================================
-- T18: check MVs are non-empty
-- =============================================================
SELECT
  'T18_non_empty_mvs' AS test_name,
  (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv)     AS job_run_rows,
  (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.all_purpose_cluster_sizing_mv) AS ap_rows,
  (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.job_compute_sizing_mv)         AS jc_rows,
  (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.right_sizing_analysis_mv)       AS rst_rows,
  (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.instance_workload_analysis_mv) AS iwp_rows,
  CASE
    WHEN (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.job_run_cost_analysis_mv) > 0
     AND (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.right_sizing_analysis_mv)  > 0
     AND (SELECT COUNT(*) FROM ${source_catalog}.${analytics_schema}.instance_workload_analysis_mv) > 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS result;
