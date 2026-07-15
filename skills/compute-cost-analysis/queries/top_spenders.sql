-- Top jobs by cost (classic + serverless), with DBU, runs, and avg duration.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  COALESCE(job_name, CONCAT('Job ', job_id))                   AS job_label,
  job_id,
  CASE WHEN is_serverless THEN 'Serverless' ELSE 'Classic' END AS compute_type,
  COUNT(DISTINCT job_run_id)             AS run_count,
  ROUND(AVG(run_duration_minutes), 1)    AS avg_duration_min,
  ROUND(SUM(total_dbus), 2)              AS total_dbus,
  ROUND(SUM(total_cost_usd), 2)          AS total_cost_usd
FROM {{catalog}}.{{schema}}.job_run_cost_analysis_mv
GROUP BY job_id, job_name, is_serverless
ORDER BY total_cost_usd DESC
LIMIT 50;
