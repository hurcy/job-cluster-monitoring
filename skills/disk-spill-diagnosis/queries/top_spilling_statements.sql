-- Statements that spilled to local disk, worst first, with root cause + fix hint.
-- Read-only. Substitute {{catalog}}/{{schema}} before running.
SELECT
  spill_date,
  compute_label,
  is_job_attributed,
  job_id, job_name, warehouse_id, cluster_id,
  statement_type, executed_by,
  ROUND(spill_gb, 2)   AS spill_gb,
  ROUND(read_gb, 2)    AS read_gb,
  ROUND(shuffle_gb, 2) AS shuffle_gb,
  read_files, pruned_files, exec_s,
  root_cause, root_cause_detail,
  statement_id, statement_text
FROM {{catalog}}.{{schema}}.query_spill_v
ORDER BY spill_gb DESC
LIMIT 100;
