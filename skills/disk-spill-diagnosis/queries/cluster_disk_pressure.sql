-- Clusters running low on local disk or swapping (includes non-SQL Spark jobs).
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  p.sample_date,
  p.cluster_id,
  n.cluster_name,
  p.samples,
  ROUND(p.avg_mem_pct, 1)      AS avg_mem_pct,
  ROUND(p.max_swap_pct, 1)     AS max_swap_pct,
  ROUND(p.min_disk_free_gb, 1) AS min_disk_free_gb,
  p.mins_disk_low,
  p.pressure_signal
FROM {{catalog}}.{{schema}}.cluster_disk_pressure_v p
LEFT JOIN {{catalog}}.{{schema}}.cluster_names n
  ON p.cluster_id = n.cluster_id
WHERE p.pressure_signal <> 'OK'
ORDER BY p.min_disk_free_gb ASC;
