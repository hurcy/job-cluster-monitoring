-- Full utilization profile + recommendation for ONE target.
-- Read-only. Substitute {{catalog}}/{{schema}}, then set the id below:
--   * All-Purpose cluster  -> fill <CLUSTER_ID> (string), leave the job_id line commented
--   * Job Compute          -> fill <JOB_ID> (numeric), use the job_id line instead
SELECT *
FROM {{catalog}}.{{schema}}.right_sizing_analysis_mv
WHERE cluster_id = '<CLUSTER_ID>'
--  OR job_id   =  <JOB_ID>
;

-- Optional per-node drill-down (why the aggregate looks the way it does):
-- SELECT instance_id, driver, node_type, avg_cpu_util, max_cpu_util, avg_mem_util,
--        max_mem_util, avg_cpu_wait, workload_profile
-- FROM {{catalog}}.{{schema}}.instance_workload_analysis_mv
-- WHERE cluster_id = '<CLUSTER_ID>'
-- ORDER BY driver, avg_cpu_util DESC;
