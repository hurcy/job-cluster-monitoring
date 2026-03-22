-- =====================================================================
-- System Table Filtered Copies - Lakeflow Declarative Pipeline
-- =====================================================================
-- system 카탈로그의 테이블에서 특정 워크스페이스(workspace_id)에 해당하는
-- 행만 필터링하여 MV로 생성한다.
--
-- Target catalog/schema: pipeline target (hurcy.default)
-- 필터 기준: workspace_id = ${workspace_id}
--
-- Pipeline Configuration Parameters:
--   workspace_id - 필터링 대상 워크스페이스 ID (STRING)
-- =====================================================================


-- =================================================================
-- 1. node_timeline  [MATERIALIZED VIEW]
-- =================================================================
-- system.compute.node_timeline에서 워크스페이스 필터링.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW node_timeline
AS
SELECT *
FROM system.compute.node_timeline
WHERE workspace_id = '${workspace_id}';


-- =================================================================
-- 2. clusters  [MATERIALIZED VIEW]
-- =================================================================
-- system.compute.clusters에서 워크스페이스 필터링.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW clusters
AS
SELECT *
FROM system.compute.clusters
WHERE workspace_id = '${workspace_id}';


-- =================================================================
-- 3. job_task_run_timeline  [MATERIALIZED VIEW]
-- =================================================================
-- system.lakeflow.job_task_run_timeline에서 워크스페이스 필터링.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW job_task_run_timeline
AS
SELECT *
FROM system.lakeflow.job_task_run_timeline
WHERE workspace_id = '${workspace_id}';


-- =================================================================
-- 4. jobs  [MATERIALIZED VIEW]
-- =================================================================
-- system.lakeflow.jobs에서 워크스페이스 필터링.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW jobs
AS
SELECT *
FROM system.lakeflow.jobs
WHERE workspace_id = '${workspace_id}';


-- =================================================================
-- 5. usage  [MATERIALIZED VIEW]
-- =================================================================
-- system.billing.usage에서 워크스페이스 필터링.
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW usage
AS
SELECT *
FROM system.billing.usage
WHERE workspace_id = '${workspace_id}';


-- =================================================================
-- 6. list_prices  [MATERIALIZED VIEW]
-- =================================================================
-- system.billing.list_prices (워크스페이스 무관, 전체 복사).
-- =================================================================

CREATE OR REFRESH MATERIALIZED VIEW list_prices
AS
SELECT *
FROM system.billing.list_prices;
