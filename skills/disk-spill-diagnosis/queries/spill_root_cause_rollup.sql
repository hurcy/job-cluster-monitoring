-- Spill volume grouped by root cause and compute type — where to focus tuning first.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  root_cause,
  compute_label,
  COUNT(*)                AS statements,
  ROUND(SUM(spill_gb), 1) AS total_spill_gb,
  ROUND(AVG(spill_gb), 2) AS avg_spill_gb,
  COUNT(DISTINCT job_id)  AS jobs_affected
FROM {{catalog}}.{{schema}}.query_spill_v
GROUP BY root_cause, compute_label
ORDER BY total_spill_gb DESC;
