-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - AUTO-RESTART SETUP (Lightweight Solution)
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Enable automatic restart from failure point for Snowflake Native dbt
-- 
-- Solution: Leverages existing DBT_MODEL_LOG as external state store to overcome
--           Snowflake Native dbt's stateless limitation (no persistent target/ dir)
-- 
-- Components Created:
--   1. RUN_DBT_AUTO_RESTART - Stored procedure with smart retry logic
--   2. Updated O2C_DAILY_BUILD task - Calls procedure instead of direct dbt
-- 
-- How It Works:
--   - Every run: Procedure checks DBT_MODEL_LOG for previous failures
--   - If failures found: Selective retry (failed models + downstream only)
--   - If no failures: Normal full build
--   - Zero changes to dbt project code required
-- 
-- Prerequisites:
--   - O2C_ENHANCED_AUDIT_SETUP.sql executed (DBT_MODEL_LOG must exist)
--   - O2C_ENHANCED_SCHEDULING_SETUP.sql executed (tasks exist)
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_AUDIT;

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '🚀 STARTING: Auto-Restart Setup for Snowflake Native dbt' AS status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: CREATE AUTO-RESTART STORED PROCEDURE
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- This procedure acts as an intelligent wrapper around SYSTEM$DBT_RUN
-- It queries DBT_MODEL_LOG (external state) to determine retry strategy
-- 
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART(
    project_name VARCHAR DEFAULT 'dbt_o2c_enhanced',
    max_retry_age_hours INTEGER DEFAULT 24
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Lightweight auto-restart: Checks DBT_MODEL_LOG for failures and selectively retries failed models + downstream'
AS
$$
DECLARE
    last_run_status VARCHAR;
    last_run_time TIMESTAMP_NTZ;
    failed_models_selector VARCHAR;
    retry_count INTEGER;
    current_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    result_message VARCHAR;
BEGIN
    -- ═══════════════════════════════════════════════════════════════════════
    -- QUERY EXTERNAL STATE: Check DBT_MODEL_LOG for recent failures
    -- ═══════════════════════════════════════════════════════════════════════
    
    SELECT 
        r.run_status,
        r.run_started_at,
        LISTAGG(DISTINCT m.model_name || '+', ' ') WITHIN GROUP (ORDER BY m.model_name),
        COUNT(DISTINCT r2.run_id)
    INTO 
        last_run_status,
        last_run_time,
        failed_models_selector,
        retry_count
    FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
    LEFT JOIN EDW.O2C_AUDIT.DBT_MODEL_LOG m 
        ON r.run_id = m.run_id 
        AND m.status IN ('FAIL', 'ERROR')
    LEFT JOIN EDW.O2C_AUDIT.DBT_RUN_LOG r2
        ON r2.run_started_at > r.run_started_at
        AND r2.run_started_at < :current_time
        AND r2.project_name = :project_name
    WHERE r.project_name = :project_name
      AND r.run_started_at > DATEADD('hour', -:max_retry_age_hours, :current_time)
    GROUP BY r.run_status, r.run_started_at
    ORDER BY r.run_started_at DESC
    LIMIT 1;
    
    -- ═══════════════════════════════════════════════════════════════════════
    -- DECISION LOGIC: Retry vs. Full Build
    -- ═══════════════════════════════════════════════════════════════════════
    
    IF (last_run_status = 'FAILED' 
        AND failed_models_selector IS NOT NULL 
        AND retry_count < 3  -- Max 3 retry attempts
        AND DATEDIFF('hour', last_run_time, :current_time) < :max_retry_age_hours) THEN
        
        -- ═══════════════════════════════════════════════════════════════════
        -- SELECTIVE RETRY: Run only failed models + their downstream children
        -- ═══════════════════════════════════════════════════════════════════
        -- The '+' selector in dbt automatically includes downstream models
        -- Example: 'model_a+ model_b+' runs model_a, model_b, and all children
        
        result_message := '🔄 RETRY #' || (retry_count + 1) || ': Rerunning failed models: ' || failed_models_selector;
        
        CALL SYSTEM$DBT_RUN(
            :project_name,
            'dbt run --select ' || failed_models_selector
        );
        
        RETURN result_message;
        
    ELSE
        -- ═══════════════════════════════════════════════════════════════════
        -- FULL BUILD: Normal scheduled run or retry limit reached
        -- ═══════════════════════════════════════════════════════════════════
        
        result_message := '🔵 FULL BUILD: ' || 
            CASE 
                WHEN retry_count >= 3 THEN 'Max retries reached, reset with full build'
                WHEN last_run_status = 'SUCCESS' THEN 'Normal scheduled run'
                WHEN last_run_status IS NULL THEN 'First run'
                WHEN DATEDIFF('hour', last_run_time, :current_time) >= :max_retry_age_hours THEN 'Failure too old, fresh start'
                ELSE 'Starting fresh build'
            END;
        
        CALL SYSTEM$DBT_RUN(:project_name, 'dbt build');
        
        RETURN result_message;
    END IF;
    
EXCEPTION
    WHEN OTHER THEN
        -- Fallback: If procedure fails, try full build
        CALL SYSTEM$DBT_RUN(:project_name, 'dbt build');
        RETURN '⚠️ ERROR in retry logic: ' || SQLERRM || ' | Executed full build as fallback';
END;
$$;

SELECT '✅ STEP 1 COMPLETE: Stored procedure RUN_DBT_AUTO_RESTART created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

GRANT USAGE ON PROCEDURE EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART(VARCHAR, INTEGER) 
    TO ROLE DBT_O2C_DEVELOPER;

GRANT USAGE ON PROCEDURE EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART(VARCHAR, INTEGER) 
    TO ROLE DBT_O2C_PROD;

SELECT '✅ STEP 2 COMPLETE: Permissions granted' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: UPDATE TASKS TO USE AUTO-RESTART PROCEDURE
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Replace direct SYSTEM$DBT_RUN calls with the smart wrapper procedure
-- 
-- ═══════════════════════════════════════════════════════════════════════════════

-- Suspend tasks before modification
ALTER TASK IF EXISTS O2C_POST_BUILD_TESTS SUSPEND;
ALTER TASK IF EXISTS O2C_PARTITION_RELOAD SUSPEND;
ALTER TASK IF EXISTS O2C_HOURLY_INCREMENTAL SUSPEND;
ALTER TASK IF EXISTS O2C_DAILY_BUILD SUSPEND;

-- Update main daily build task with auto-restart capability
CREATE OR REPLACE TASK O2C_DAILY_BUILD
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- 6 AM UTC daily
    COMMENT = 'Daily full build with auto-restart from failure point'
AS
    -- ⭐ Smart wrapper: Automatically detects and retries failures
    CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced', 24);

COMMENT ON TASK O2C_DAILY_BUILD IS 
    'Runs dbt build at 6 AM UTC with automatic restart from failure point. Uses RUN_DBT_AUTO_RESTART procedure.';

SELECT '✅ STEP 3 COMPLETE: O2C_DAILY_BUILD task updated with auto-restart' AS status;

-- Update hourly incremental task (optional - can also use auto-restart)
CREATE OR REPLACE TASK O2C_HOURLY_INCREMENTAL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Top of every hour
    COMMENT = 'Hourly incremental refresh with auto-restart'
AS
    CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced', 2);  -- Only retry within 2 hours

COMMENT ON TASK O2C_HOURLY_INCREMENTAL IS 
    'Runs incremental models hourly with auto-restart capability';

SELECT '✅ STEP 3b COMPLETE: O2C_HOURLY_INCREMENTAL task updated' AS status;

-- Recreate dependent tasks (unchanged logic, just ensuring they exist)
CREATE OR REPLACE TASK O2C_POST_BUILD_TESTS
    WAREHOUSE = COMPUTE_WH
    AFTER O2C_DAILY_BUILD
    COMMENT = 'Run dbt tests after daily build'
AS
    CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt test');

CREATE OR REPLACE TASK O2C_PARTITION_RELOAD
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 7 * * * UTC'
    COMMENT = 'Reload partitioned facts for last 3 days'
AS
    CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt run --select tag:delete_insert --vars ''{"reload_days": 3}''');

SELECT '✅ STEP 3c COMPLETE: Dependent tasks recreated' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: RESUME TASKS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Resume in correct order (parent first, then children)
ALTER TASK O2C_DAILY_BUILD RESUME;
ALTER TASK O2C_POST_BUILD_TESTS RESUME;
ALTER TASK O2C_PARTITION_RELOAD RESUME;

-- Optional: Resume hourly task if needed
-- ALTER TASK O2C_HOURLY_INCREMENTAL RESUME;

SELECT '✅ STEP 4 COMPLETE: Tasks resumed' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: CREATE MONITORING VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY AS
SELECT 
    r.run_id,
    r.run_started_at,
    r.run_ended_at,
    r.run_duration_seconds,
    r.run_status,
    r.models_run,
    r.models_failed,
    -- Detect if this was a retry or full build
    CASE 
        WHEN r.models_run < 10 THEN '🔄 SELECTIVE RETRY'
        WHEN r.models_run >= 10 THEN '🔵 FULL BUILD'
        ELSE '❓ UNKNOWN'
    END AS run_type,
    -- Get list of models that ran
    LISTAGG(DISTINCT m.model_name, ', ') WITHIN GROUP (ORDER BY m.model_name) AS models_executed,
    -- Get list of failed models if any
    LISTAGG(DISTINCT CASE WHEN m.status IN ('FAIL', 'ERROR') THEN m.model_name END, ', ') 
        WITHIN GROUP (ORDER BY m.model_name) AS failed_models,
    -- Calculate retry sequence
    ROW_NUMBER() OVER (PARTITION BY 
        CASE WHEN r.models_run < 10 THEN 'retry_group' ELSE run_id END 
        ORDER BY r.run_started_at) AS retry_sequence
FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
LEFT JOIN EDW.O2C_AUDIT.DBT_MODEL_LOG m ON r.run_id = m.run_id
WHERE r.project_name = 'dbt_o2c_enhanced'
GROUP BY r.run_id, r.run_started_at, r.run_ended_at, r.run_duration_seconds, 
         r.run_status, r.models_run, r.models_failed
ORDER BY r.run_started_at DESC;

COMMENT ON VIEW EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY IS
    'Shows run history with auto-restart detection (retry vs full build)';

SELECT '✅ STEP 5 COMPLETE: Monitoring view V_AUTO_RESTART_HISTORY created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION & STATUS
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ AUTO-RESTART SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Show current task status
SELECT 
    name AS task_name,
    state AS task_state,
    schedule,
    comment
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'EDW.O2C_AUDIT.O2C_DAILY_BUILD',
    RECURSIVE => TRUE
))
WHERE database_name = 'EDW'
UNION ALL
SELECT 
    'O2C_DAILY_BUILD' AS task_name,
    state,
    schedule,
    comment
FROM INFORMATION_SCHEMA.TASKS
WHERE task_name = 'O2C_DAILY_BUILD'
  AND task_schema = 'O2C_AUDIT'
  AND task_catalog = 'EDW';

-- ═══════════════════════════════════════════════════════════════════════════════
-- TESTING INSTRUCTIONS
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '🧪 TESTING INSTRUCTIONS' AS section;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

/*

-- TEST 1: Manual execution (should run full build on first run)
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');
-- Expected: "🔵 FULL BUILD: First run" or "Normal scheduled run"

-- TEST 2: Simulate a failure
INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (
    log_id, run_id, project_name, model_name, schema_name, database_name,
    materialization, batch_id, status, started_at, ended_at, is_incremental
) VALUES (
    'test-fail-001', 'test-run-001', 'dbt_o2c_enhanced', 'dm_o2c_reconciliation',
    'O2C_ENHANCED_CORE', 'EDW', 'incremental', 'batch-001', 'FAIL',
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE
);

INSERT INTO EDW.O2C_AUDIT.DBT_RUN_LOG (
    run_id, project_name, environment, run_started_at, run_status, models_run, models_failed
) VALUES (
    'test-run-001', 'dbt_o2c_enhanced', 'prod', CURRENT_TIMESTAMP(), 'FAILED', 25, 1
);

-- TEST 3: Run auto-restart procedure (should detect failure and retry selectively)
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');
-- Expected: "🔄 RETRY #1: Rerunning failed models: dm_o2c_reconciliation+"

-- TEST 4: Check monitoring view
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY LIMIT 10;
-- Should show run_type as '🔄 SELECTIVE RETRY' or '🔵 FULL BUILD'

-- TEST 5: Manually trigger task
EXECUTE TASK O2C_DAILY_BUILD;

-- TEST 6: Check task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'O2C_DAILY_BUILD',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;

-- CLEANUP TEST DATA:
DELETE FROM EDW.O2C_AUDIT.DBT_MODEL_LOG WHERE log_id = 'test-fail-001';
DELETE FROM EDW.O2C_AUDIT.DBT_RUN_LOG WHERE run_id = 'test-run-001';

*/

-- ═══════════════════════════════════════════════════════════════════════════════
-- MONITORING QUERIES (Reference)
-- ═══════════════════════════════════════════════════════════════════════════════

/*

-- View recent runs with retry detection
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY LIMIT 20;

-- Check for retry patterns
SELECT 
    DATE(run_started_at) AS run_date,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN run_type = '🔄 SELECTIVE RETRY' THEN 1 ELSE 0 END) AS retry_runs,
    SUM(CASE WHEN run_type = '🔵 FULL BUILD' THEN 1 ELSE 0 END) AS full_builds,
    SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs
FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY
GROUP BY DATE(run_started_at)
ORDER BY run_date DESC;

-- View procedure execution details
SELECT 
    query_id,
    query_text,
    start_time,
    end_time,
    execution_status,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%RUN_DBT_AUTO_RESTART%'
  AND start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;

*/

SELECT '✅ Setup complete! Auto-restart is now active on scheduled tasks.' AS final_message;
SELECT 'ℹ️  The procedure will automatically detect failures and retry only affected models.' AS info;
SELECT 'ℹ️  No changes to your dbt project code required.' AS info2;

