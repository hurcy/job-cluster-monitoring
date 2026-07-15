-- Top downsizing candidates across ALL classic compute, ranked by estimated savings.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  compute_type,
  COALESCE(cluster_name, job_name, CONCAT('Job ', job_id),
           CONCAT('Cluster ', cluster_id))                               AS target,
  cluster_id,
  job_id,
  owned_by,
  worker_node_type,
  COALESCE(CAST(configured_workers AS STRING),
           CONCAT(CAST(min_autoscale_workers AS STRING), '-',
                  CAST(max_autoscale_workers AS STRING)))                AS workers_config,
  run_count,
  ROUND(avg_cpu_util, 1)                                                 AS avg_cpu_pct,
  ROUND(median_cpu_util, 1)                                              AS median_cpu_pct,
  ROUND(avg_mem_util, 1)                                                 AS avg_mem_pct,
  sizing_recommendation,
  ROUND(total_cost_usd, 2)                                              AS total_cost_usd,
  ROUND(estimated_savings_usd, 2)                                       AS estimated_savings_usd,
  recommendation_detail
FROM {{catalog}}.{{schema}}.right_sizing_analysis_mv
WHERE sizing_recommendation IN ('DEFINITE_DOWNSIZE', 'LIKELY_DOWNSIZE')
ORDER BY estimated_savings_usd DESC;
