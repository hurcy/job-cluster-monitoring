-- Clusters/jobs under resource pressure (upsize candidates) with the driving signal.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
-- Driver priority in recommendation_detail: memory P95 / swap / disk spill / CPU P95.
SELECT
  compute_type,
  COALESCE(cluster_name, job_name, CONCAT('Job ', job_id),
           CONCAT('Cluster ', cluster_id))          AS target,
  cluster_id,
  job_id,
  owned_by,
  worker_node_type,
  run_count,
  ROUND(p95_cpu_util, 1)                            AS p95_cpu_pct,
  ROUND(p95_mem_util, 1)                            AS p95_mem_pct,
  ROUND(max_swap_pct, 1)                            AS max_swap_pct,
  ROUND(spill_gb, 1)                                AS spill_gb,
  ROUND(total_cost_usd, 2)                          AS total_cost_usd,
  sizing_recommendation,
  recommendation_detail
FROM {{catalog}}.{{schema}}.right_sizing_analysis_mv
WHERE sizing_recommendation = 'CONSIDER_UPSIZE'
ORDER BY spill_gb DESC, max_swap_pct DESC, p95_mem_util DESC;
