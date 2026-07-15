-- Total cost + DBU split by classic vs serverless.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  CASE WHEN is_serverless THEN 'Serverless' ELSE 'Classic' END AS compute_type,
  COUNT(DISTINCT job_id)        AS jobs,
  COUNT(DISTINCT job_run_id)    AS runs,
  ROUND(SUM(total_dbus), 2)     AS total_dbus,
  ROUND(SUM(total_cost_usd), 2) AS total_cost_usd
FROM {{catalog}}.{{schema}}.job_run_cost_analysis_mv
GROUP BY is_serverless
ORDER BY total_cost_usd DESC;
