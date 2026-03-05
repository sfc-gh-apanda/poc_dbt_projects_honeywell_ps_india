-- ═══════════════════════════════════════════════════════════════════════════════
-- DWS AUDIT & OBSERVABILITY SETUP
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Purpose: Create audit/monitoring tables for dbt run tracking
-- Tables:  DBT_RUN_LOG, DBT_MODEL_LOG
-- Schema:  DWSEDW.DWS_AUDIT
--
-- Run this ONCE before first dbt build.
--
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;
USE DATABASE DWSEDW;

CREATE SCHEMA IF NOT EXISTS DWSEDW.DWS_AUDIT;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 1: DBT_RUN_LOG - Tracks each dbt run (invocation)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS DWSEDW.DWS_AUDIT.DBT_RUN_LOG (
    run_id              VARCHAR(100)    NOT NULL,
    project_name        VARCHAR(100),
    project_version     VARCHAR(20),
    environment         VARCHAR(20),
    run_started_at      TIMESTAMP_NTZ,
    run_ended_at        TIMESTAMP_NTZ,
    run_duration_seconds NUMBER,
    run_status          VARCHAR(20),        -- RUNNING, SUCCESS, FAILED, ERROR
    run_command         VARCHAR(200),
    warehouse_name      VARCHAR(100),
    user_name           VARCHAR(100),
    role_name           VARCHAR(100),
    selector_used       VARCHAR(500),
    models_run          NUMBER DEFAULT 0,
    models_success      NUMBER DEFAULT 0,
    models_failed       NUMBER DEFAULT 0,
    models_skipped      NUMBER DEFAULT 0,
    PRIMARY KEY (run_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 2: DBT_MODEL_LOG - Tracks each model execution
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS DWSEDW.DWS_AUDIT.DBT_MODEL_LOG (
    log_id              VARCHAR(200)    NOT NULL,
    run_id              VARCHAR(100)    NOT NULL,
    project_name        VARCHAR(100),
    model_name          VARCHAR(200),
    model_alias         VARCHAR(200),
    schema_name         VARCHAR(100),
    database_name       VARCHAR(100),
    materialization     VARCHAR(30),
    batch_id            VARCHAR(200),
    status              VARCHAR(20),        -- SUCCESS, FAIL, ERROR, SKIPPED
    error_message       VARCHAR(2000),
    started_at          TIMESTAMP_NTZ,
    ended_at            TIMESTAMP_NTZ,
    rows_affected       NUMBER,
    is_incremental      BOOLEAN DEFAULT FALSE,
    incremental_strategy VARCHAR(30),
    PRIMARY KEY (log_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- MONITORING VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW DWSEDW.DWS_AUDIT.V_RUN_HISTORY AS
SELECT
    run_id,
    project_name,
    environment,
    run_status,
    run_started_at,
    run_ended_at,
    run_duration_seconds,
    models_run,
    models_success,
    models_failed,
    ROUND(models_success * 100.0 / NULLIF(models_run, 0), 1) AS success_rate_pct,
    warehouse_name,
    user_name
FROM DWSEDW.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC;

CREATE OR REPLACE VIEW DWSEDW.DWS_AUDIT.V_MODEL_PERFORMANCE AS
SELECT
    model_name,
    materialization,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status IN ('FAIL','ERROR') THEN 1 ELSE 0 END) AS failed_runs,
    AVG(rows_affected) AS avg_rows_affected,
    MAX(ended_at) AS last_run_at
FROM DWSEDW.DWS_AUDIT.DBT_MODEL_LOG
GROUP BY model_name, materialization
ORDER BY model_name;

CREATE OR REPLACE VIEW DWSEDW.DWS_AUDIT.V_RECENT_FAILURES AS
SELECT
    m.run_id,
    m.model_name,
    m.materialization,
    m.status,
    m.error_message,
    m.started_at,
    r.environment
FROM DWSEDW.DWS_AUDIT.DBT_MODEL_LOG m
JOIN DWSEDW.DWS_AUDIT.DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.status IN ('FAIL', 'ERROR')
ORDER BY m.started_at DESC
LIMIT 50;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT 'DWS_AUDIT tables created successfully' AS status;
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DWS_AUDIT'
ORDER BY TABLE_NAME;
