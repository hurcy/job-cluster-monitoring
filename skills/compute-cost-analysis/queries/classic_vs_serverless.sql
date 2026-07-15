-- Daily cost + DBU trend, classic vs serverless.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  DATE(run_start_time)                                         AS run_date,
  CASE WHEN is_serverless THEN 'Serverless' ELSE 'Classic' END AS compute_type,
  ROUND(SUM(total_cost_usd), 2) AS cost_usd,
  ROUND(SUM(total_dbus), 2)     AS dbus
FROM {{catalog}}.{{schema}}.job_run_cost_analysis_mv
GROUP BY DATE(run_start_time), is_serverless
ORDER BY run_date, compute_type;
