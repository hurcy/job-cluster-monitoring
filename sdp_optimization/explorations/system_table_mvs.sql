-- =====================================================================
-- System Table MV Copies
-- =====================================================================
-- 보안 요구사항으로 system 카탈로그에 직접 접근할 수 없는 환경을 위해
-- system 테이블의 MV 사본을 별도 catalog.schema에 생성한다.
--
-- 대상 카탈로그: hurcy (파이프라인 target catalog과 동일)
-- 스키마: compute, lakeflow, billing (system 카탈로그 구조를 미러링)
--
-- 실행 방법: SQL Warehouse에서 직접 실행 (DLT 파이프라인 외부)
-- =====================================================================


-- =================================================================
-- 스키마 생성
-- =================================================================

CREATE SCHEMA IF NOT EXISTS hurcy.compute;
CREATE SCHEMA IF NOT EXISTS hurcy.lakeflow;
CREATE SCHEMA IF NOT EXISTS hurcy.billing;


-- =================================================================
-- system.compute 테이블 사본
-- =================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS hurcy.compute.node_timeline
AS SELECT * FROM system.compute.node_timeline;

CREATE MATERIALIZED VIEW IF NOT EXISTS hurcy.compute.clusters
AS SELECT * FROM system.compute.clusters;


-- =================================================================
-- system.lakeflow 테이블 사본
-- =================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS hurcy.lakeflow.job_task_run_timeline
AS SELECT * FROM system.lakeflow.job_task_run_timeline;

CREATE MATERIALIZED VIEW IF NOT EXISTS hurcy.lakeflow.jobs
AS SELECT * FROM system.lakeflow.jobs;


-- =================================================================
-- system.billing 테이블 사본
-- =================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS hurcy.billing.usage
AS SELECT * FROM system.billing.usage;

CREATE MATERIALIZED VIEW IF NOT EXISTS hurcy.billing.list_prices
AS SELECT * FROM system.billing.list_prices;
