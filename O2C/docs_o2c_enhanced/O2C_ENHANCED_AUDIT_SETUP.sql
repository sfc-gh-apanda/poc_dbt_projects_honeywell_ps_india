-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - AUDIT TRACKING TABLES SETUP
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Create processing tracking tables for dbt audit and observability
-- 
-- Tables Created:
--   1. DBT_RUN_LOG         - One row per dbt run (invocation)
--   2. DBT_MODEL_LOG       - One row per model per run
--   3. DBT_BATCH_LINEAGE   - Batch-to-batch data lineage (optional)
-- 
-- Prerequisites:
--   - Run with ACCOUNTADMIN or equivalent
--   - Database EDW must exist
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: CREATE AUDIT SCHEMA
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS O2C_AUDIT
    COMMENT = 'O2C Enhanced - Processing tracking and audit tables';

USE SCHEMA O2C_AUDIT;

SELECT '✅ STEP 1 COMPLETE: O2C_AUDIT schema created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: CREATE DBT_RUN_LOG TABLE
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Track each dbt run (invocation) at a high level
-- Populated by: on-run-start and on-run-end hooks

CREATE TABLE IF NOT EXISTS DBT_RUN_LOG (
    -- Primary Key
    run_id                  VARCHAR(50) PRIMARY KEY COMMENT 'dbt invocation_id',
    
    -- Project Info
    project_name            VARCHAR(100) COMMENT 'dbt project name',
    project_version         VARCHAR(20) COMMENT 'dbt version',
    environment             VARCHAR(20) COMMENT 'Target environment (dev/prod)',
    
    -- Timing
    run_started_at          TIMESTAMP_NTZ COMMENT 'Run start timestamp',
    run_ended_at            TIMESTAMP_NTZ COMMENT 'Run end timestamp',
    run_duration_seconds    NUMBER(10,2) COMMENT 'Total run duration in seconds',
    
    -- Status
    run_status              VARCHAR(20) COMMENT 'RUNNING, SUCCESS, FAILED, PARTIAL',
    run_command             VARCHAR(50) COMMENT 'dbt command (run, build, test)',
    
    -- Counts
    models_selected         INTEGER COMMENT 'Models selected for run',
    models_run              INTEGER COMMENT 'Models actually run',
    models_success          INTEGER COMMENT 'Models succeeded',
    models_failed           INTEGER COMMENT 'Models failed',
    models_skipped          INTEGER COMMENT 'Models skipped',
    tests_run               INTEGER COMMENT 'Tests run',
    tests_passed            INTEGER COMMENT 'Tests passed',
    tests_failed            INTEGER COMMENT 'Tests failed',
    
    -- Execution Context
    warehouse_name          VARCHAR(100) COMMENT 'Snowflake warehouse used',
    user_name               VARCHAR(100) COMMENT 'Snowflake user',
    role_name               VARCHAR(100) COMMENT 'Snowflake role',
    
    -- Selector Info
    selector_used           VARCHAR(500) COMMENT 'dbt selector/filter used',
    
    -- Audit
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation time'
);

COMMENT ON TABLE DBT_RUN_LOG IS 
    'dbt run-level tracking - one row per dbt invocation';

SELECT '✅ STEP 2 COMPLETE: DBT_RUN_LOG table created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: CREATE DBT_MODEL_LOG TABLE
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Track each model execution within a run
-- Populated by: post-hook on each model

CREATE TABLE IF NOT EXISTS DBT_MODEL_LOG (
    -- Primary Key
    log_id                  VARCHAR(100) PRIMARY KEY COMMENT 'Unique log entry ID',
    
    -- Foreign Key to Run
    run_id                  VARCHAR(50) COMMENT 'dbt invocation_id (FK to DBT_RUN_LOG)',
    
    -- Model Info
    project_name            VARCHAR(100) COMMENT 'dbt project name',
    model_name              VARCHAR(200) COMMENT 'Model name',
    model_alias             VARCHAR(200) COMMENT 'Model alias (if different)',
    schema_name             VARCHAR(100) COMMENT 'Target schema',
    database_name           VARCHAR(100) COMMENT 'Target database',
    materialization         VARCHAR(50) COMMENT 'table, view, incremental, etc.',
    
    -- Batch Tracking
    batch_id                VARCHAR(50) COMMENT 'Unique batch ID per model per run',
    parent_batch_id         VARCHAR(50) COMMENT 'Parent batch ID (for lineage)',
    
    -- Status
    status                  VARCHAR(20) COMMENT 'SUCCESS, FAIL, SKIP, ERROR',
    error_message           VARCHAR(4000) COMMENT 'Error message if failed',
    
    -- Timing
    started_at              TIMESTAMP_NTZ COMMENT 'Model execution start',
    ended_at                TIMESTAMP_NTZ COMMENT 'Model execution end',
    execution_seconds       NUMBER(10,2) COMMENT 'Execution duration',
    
    -- Data Metrics
    rows_affected           INTEGER COMMENT 'Rows produced/affected',
    bytes_processed         INTEGER COMMENT 'Bytes scanned',
    
    -- Incremental Info
    is_incremental          BOOLEAN COMMENT 'Is this an incremental model?',
    incremental_strategy    VARCHAR(50) COMMENT 'merge, append, delete+insert, etc.',
    
    -- Query Info
    query_id                VARCHAR(50) COMMENT 'Snowflake query ID',
    
    -- Audit
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation time'
);

COMMENT ON TABLE DBT_MODEL_LOG IS 
    'dbt model-level tracking - one row per model per run';

-- Note: Snowflake standard tables do not support indexes.
-- For performance optimization on large tables, consider:
-- 1. Using CLUSTER BY (run_id, model_name) on table creation
-- 2. Using Search Optimization Service for point lookups
-- Example: ALTER TABLE DBT_MODEL_LOG ADD SEARCH OPTIMIZATION ON EQUALITY(run_id, model_name, batch_id);

SELECT '✅ STEP 3 COMPLETE: DBT_MODEL_LOG table created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: CREATE DBT_BATCH_LINEAGE TABLE (Optional - for advanced lineage)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Track batch-to-batch data lineage
-- Populated by: Custom macros when detailed lineage is needed

CREATE TABLE IF NOT EXISTS DBT_BATCH_LINEAGE (
    -- Primary Key
    lineage_id              VARCHAR(50) PRIMARY KEY COMMENT 'Unique lineage entry ID',
    
    -- Batch Info
    run_id                  VARCHAR(50) COMMENT 'dbt invocation_id',
    batch_id                VARCHAR(50) COMMENT 'Target batch ID',
    source_batch_id         VARCHAR(50) COMMENT 'Source batch ID',
    
    -- Model Info
    source_project          VARCHAR(100) COMMENT 'Source dbt project',
    source_model            VARCHAR(200) COMMENT 'Source model name',
    target_project          VARCHAR(100) COMMENT 'Target dbt project',
    target_model            VARCHAR(200) COMMENT 'Target model name',
    
    -- Processing Info
    rows_read               INTEGER COMMENT 'Rows read from source',
    rows_written            INTEGER COMMENT 'Rows written to target',
    rows_rejected           INTEGER COMMENT 'Rows rejected',
    
    -- Timing
    processing_started_at   TIMESTAMP_NTZ COMMENT 'Processing start time',
    processing_ended_at     TIMESTAMP_NTZ COMMENT 'Processing end time',
    
    -- Status
    processing_status       VARCHAR(20) COMMENT 'SUCCESS, PARTIAL, FAILED',
    
    -- Audit
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation time'
);

COMMENT ON TABLE DBT_BATCH_LINEAGE IS 
    'Batch-to-batch data lineage tracking';

-- Note: Snowflake standard tables do not support indexes.
-- For performance optimization, consider Search Optimization Service if needed.

SELECT '✅ STEP 4 COMPLETE: DBT_BATCH_LINEAGE table created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: CREATE SUMMARY VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════

-- View: Daily Run Summary
CREATE OR REPLACE VIEW V_DAILY_RUN_SUMMARY AS
SELECT
    DATE(run_started_at) AS run_date,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    SUM(models_run) AS total_models_run,
    SUM(models_success) AS total_models_success,
    SUM(models_failed) AS total_models_failed,
    ROUND(AVG(run_duration_seconds), 2) AS avg_run_duration_sec,
    ROUND(SUM(run_duration_seconds), 2) AS total_run_duration_sec
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

COMMENT ON VIEW V_DAILY_RUN_SUMMARY IS 
    'Daily summary of dbt runs for the last 30 days';

-- View: Model Execution History
CREATE OR REPLACE VIEW V_MODEL_EXECUTION_HISTORY AS
SELECT
    m.model_name,
    m.schema_name,
    m.materialization,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN m.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN m.status = 'FAIL' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(AVG(m.execution_seconds), 2) AS avg_execution_sec,
    ROUND(MAX(m.execution_seconds), 2) AS max_execution_sec,
    AVG(m.rows_affected) AS avg_rows_affected,
    MAX(m.started_at) AS last_run_at,
    r.environment
FROM DBT_MODEL_LOG m
LEFT JOIN DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.started_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY m.model_name, m.schema_name, m.materialization, r.environment
ORDER BY total_runs DESC;

COMMENT ON VIEW V_MODEL_EXECUTION_HISTORY IS 
    'Model execution statistics for the last 7 days';

-- View: Recent Failures
CREATE OR REPLACE VIEW V_RECENT_FAILURES AS
SELECT
    m.run_id,
    m.model_name,
    m.schema_name,
    m.status,
    m.error_message,
    m.started_at,
    m.execution_seconds,
    r.environment,
    r.user_name
FROM DBT_MODEL_LOG m
LEFT JOIN DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.status IN ('FAIL', 'ERROR')
  AND m.started_at >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY m.started_at DESC;

COMMENT ON VIEW V_RECENT_FAILURES IS 
    'Recent model execution failures for the last 7 days';

SELECT '✅ STEP 5 COMPLETE: Summary views created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Grant to dbt developer role
GRANT USAGE ON SCHEMA EDW.O2C_AUDIT TO ROLE DBT_O2C_DEVELOPER;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_AUDIT TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.O2C_AUDIT TO ROLE DBT_O2C_DEVELOPER;

-- Grant future privileges
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_AUDIT TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA EDW.O2C_AUDIT TO ROLE DBT_O2C_DEVELOPER;

SELECT '✅ STEP 6 COMPLETE: Permissions granted' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ O2C ENHANCED AUDIT SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Show created objects
SHOW TABLES IN SCHEMA EDW.O2C_AUDIT;
SHOW VIEWS IN SCHEMA EDW.O2C_AUDIT;

-- Verification: Tables exist
SELECT 
    'DBT_RUN_LOG' AS table_name, 
    CASE WHEN COUNT(*) >= 0 THEN '✅ Ready' ELSE '❌ Missing' END AS status
FROM EDW.O2C_AUDIT.DBT_RUN_LOG
UNION ALL
SELECT 
    'DBT_MODEL_LOG', 
    CASE WHEN COUNT(*) >= 0 THEN '✅ Ready' ELSE '❌ Missing' END
FROM EDW.O2C_AUDIT.DBT_MODEL_LOG
UNION ALL
SELECT 
    'DBT_BATCH_LINEAGE', 
    CASE WHEN COUNT(*) >= 0 THEN '✅ Ready' ELSE '❌ Missing' END
FROM EDW.O2C_AUDIT.DBT_BATCH_LINEAGE;


