-- Right-sizing opportunity aggregated by cluster/job owner — where to focus first.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  owned_by,
  COUNT(*)                                                                     AS targets,
  SUM(CASE WHEN sizing_recommendation = 'DEFINITE_DOWNSIZE' THEN 1 ELSE 0 END) AS definite_downsize,
  SUM(CASE WHEN sizing_recommendation = 'LIKELY_DOWNSIZE'   THEN 1 ELSE 0 END) AS likely_downsize,
  SUM(CASE WHEN sizing_recommendation = 'CONSIDER_UPSIZE'   THEN 1 ELSE 0 END) AS consider_upsize,
  ROUND(SUM(total_cost_usd), 2)                                                AS total_cost_usd,
  ROUND(SUM(estimated_savings_usd), 2)                                         AS estimated_savings_usd
FROM {{catalog}}.{{schema}}.right_sizing_analysis_mv
GROUP BY owned_by
ORDER BY estimated_savings_usd DESC;
