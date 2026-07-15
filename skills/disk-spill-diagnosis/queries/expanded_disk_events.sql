-- Actual elastic-disk expansions (and failures to expand), most recent first.
-- DID_NOT_EXPAND_DISK with a 'cause' is the strongest disk-starvation signal.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  e.event_time,
  e.event_date,
  e.cluster_id,
  n.cluster_name,
  e.job_id, e.job_run_id,
  e.event_type,
  e.instance_id,
  ROUND(e.previous_disk_size / 1e9, 1) AS previous_disk_gb,
  ROUND(e.disk_size          / 1e9, 1) AS disk_gb,
  ROUND(e.free_space_bytes   / 1e9, 1) AS free_space_gb,
  e.cause
FROM {{catalog}}.{{schema}}.expanded_disk_events e
LEFT JOIN {{catalog}}.{{schema}}.cluster_names n
  ON e.cluster_id = n.cluster_id
ORDER BY e.event_time DESC
LIMIT 200;
