-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - SCHEDULING SETUP
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Set up Snowflake Tasks for automated dbt scheduling
-- 
-- Tasks Created:
--   1. O2C_DAILY_BUILD        - Full dbt build at 6 AM UTC daily
--   2. O2C_HOURLY_INCREMENTAL - Incremental refresh every hour
--   3. O2C_STAGING_REFRESH    - Staging layer only (every 30 min)
--   4. O2C_ALERT_TASK         - Send alerts on failures
-- 
-- Prerequisites:
--   - dbt_o2c_enhanced project deployed to Snowflake
--   - O2C_ENHANCED_AUDIT_SETUP.sql executed
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_AUDIT;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: TASK 1 - DAILY FULL BUILD
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Run full dbt build once per day

CREATE OR REPLACE TASK O2C_DAILY_BUILD
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- 6 AM UTC daily
    COMMENT = 'Daily full build of dbt_o2c_enhanced project'
AS
    CALL SYSTEM$DBT_RUN(
        'dbt_o2c_enhanced',
        'dbt build --full-refresh'
    );

COMMENT ON TASK O2C_DAILY_BUILD IS 
    'Runs dbt build --full-refresh at 6 AM UTC daily';

SELECT '✅ TASK 1 CREATED: O2C_DAILY_BUILD (6 AM UTC daily)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: TASK 2 - HOURLY INCREMENTAL
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Run incremental models every hour

CREATE OR REPLACE TASK O2C_HOURLY_INCREMENTAL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Top of every hour
    COMMENT = 'Hourly incremental refresh of core marts'
AS
    CALL SYSTEM$DBT_RUN(
        'dbt_o2c_enhanced',
        'dbt run --select tag:merge tag:append_only'
    );

COMMENT ON TASK O2C_HOURLY_INCREMENTAL IS 
    'Runs incremental models every hour';

SELECT '✅ TASK 2 CREATED: O2C_HOURLY_INCREMENTAL (hourly)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: TASK 3 - STAGING REFRESH (Optional - High Frequency)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Refresh staging views more frequently

CREATE OR REPLACE TASK O2C_STAGING_REFRESH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON */30 * * * * UTC'  -- Every 30 minutes
    COMMENT = 'Refresh staging layer every 30 minutes'
    -- Start suspended - enable if needed
    -- ALLOW_OVERLAPPING_EXECUTION = FALSE
AS
    CALL SYSTEM$DBT_RUN(
        'dbt_o2c_enhanced',
        'dbt run --select staging'
    );

COMMENT ON TASK O2C_STAGING_REFRESH IS 
    'Refreshes staging views every 30 minutes (starts SUSPENDED)';

SELECT '✅ TASK 3 CREATED: O2C_STAGING_REFRESH (30 min, SUSPENDED)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: TASK 4 - DELETE+INSERT DAILY PARTITIONS
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Reload last 3 days of partitioned fact tables

CREATE OR REPLACE TASK O2C_PARTITION_RELOAD
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 7 * * * UTC'  -- 7 AM UTC daily (after main build)
    COMMENT = 'Reload partitioned facts for last 3 days'
AS
    CALL SYSTEM$DBT_RUN(
        'dbt_o2c_enhanced',
        'dbt run --select tag:delete_insert --vars ''{"reload_days": 3}'''
    );

COMMENT ON TASK O2C_PARTITION_RELOAD IS 
    'Reloads last 3 days of partitioned fact tables at 7 AM UTC';

SELECT '✅ TASK 4 CREATED: O2C_PARTITION_RELOAD (7 AM UTC daily)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: CHAINED TASK - TESTS AFTER BUILD
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Run tests after daily build completes

CREATE OR REPLACE TASK O2C_POST_BUILD_TESTS
    WAREHOUSE = COMPUTE_WH
    AFTER O2C_DAILY_BUILD  -- Runs after daily build
    COMMENT = 'Run dbt tests after daily build'
AS
    CALL SYSTEM$DBT_RUN(
        'dbt_o2c_enhanced',
        'dbt test'
    );

COMMENT ON TASK O2C_POST_BUILD_TESTS IS 
    'Runs dbt test after O2C_DAILY_BUILD completes';

SELECT '✅ TASK 5 CREATED: O2C_POST_BUILD_TESTS (chained after daily build)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: ENABLE TASKS
-- ═══════════════════════════════════════════════════════════════════════════════
-- Note: Tasks are created in SUSPENDED state by default
-- Enable only the tasks you need

-- Enable daily build (primary task)
ALTER TASK O2C_DAILY_BUILD RESUME;

-- Enable chained test task
ALTER TASK O2C_POST_BUILD_TESTS RESUME;

-- Enable partition reload
ALTER TASK O2C_PARTITION_RELOAD RESUME;

-- NOTE: Hourly incremental and staging refresh are OPTIONAL
-- Uncomment to enable:
-- ALTER TASK O2C_HOURLY_INCREMENTAL RESUME;
-- ALTER TASK O2C_STAGING_REFRESH RESUME;

SELECT '✅ STEP 6 COMPLETE: Primary tasks enabled' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 7: TASK MONITORING VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW V_TASK_STATUS AS
SELECT
    name AS task_name,
    state,
    schedule,
    warehouse,
    database_name,
    schema_name,
    comment,
    created_on,
    -- Last run info (from task history)
    (SELECT MAX(scheduled_time) 
     FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => name))
     WHERE state = 'SUCCEEDED') AS last_success,
    (SELECT MAX(scheduled_time) 
     FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => name))
     WHERE state = 'FAILED') AS last_failure
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'O2C_DAILY_BUILD',
    RECURSIVE => TRUE
))
WHERE database_name = 'EDW'

UNION ALL

SELECT
    'O2C_DAILY_BUILD',
    state,
    schedule,
    warehouse,
    database_name,
    schema_name,
    comment,
    created_on,
    NULL,
    NULL
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'O2C_DAILY_BUILD',
    RECURSIVE => FALSE
))
WHERE 1=0;  -- Just to get the root task

COMMENT ON VIEW V_TASK_STATUS IS 
    'Current status of all O2C scheduled tasks';

SELECT '✅ STEP 7 COMPLETE: Task monitoring view created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION & STATUS
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ O2C ENHANCED SCHEDULING SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Show task status
SHOW TASKS IN SCHEMA EDW.O2C_AUDIT;

-- Task execution history (last 24 hours)
SELECT 
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF('second', scheduled_time, completed_time) AS duration_sec,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
))
WHERE database_name = 'EDW'
  AND schema_name = 'O2C_AUDIT'
ORDER BY scheduled_time DESC
LIMIT 20;

-- ═══════════════════════════════════════════════════════════════════════════════
-- MANAGEMENT COMMANDS (Reference)
-- ═══════════════════════════════════════════════════════════════════════════════

/*
-- SUSPEND ALL TASKS:
ALTER TASK O2C_POST_BUILD_TESTS SUSPEND;  -- Suspend children first
ALTER TASK O2C_PARTITION_RELOAD SUSPEND;
ALTER TASK O2C_HOURLY_INCREMENTAL SUSPEND;
ALTER TASK O2C_STAGING_REFRESH SUSPEND;
ALTER TASK O2C_DAILY_BUILD SUSPEND;       -- Suspend parent last

-- RESUME ALL TASKS:
ALTER TASK O2C_DAILY_BUILD RESUME;        -- Resume parent first
ALTER TASK O2C_POST_BUILD_TESTS RESUME;
ALTER TASK O2C_PARTITION_RELOAD RESUME;

-- MANUALLY EXECUTE A TASK:
EXECUTE TASK O2C_DAILY_BUILD;

-- CHECK TASK HISTORY:
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'O2C_DAILY_BUILD',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;

-- DROP TASKS (if needed):
DROP TASK IF EXISTS O2C_POST_BUILD_TESTS;
DROP TASK IF EXISTS O2C_PARTITION_RELOAD;
DROP TASK IF EXISTS O2C_HOURLY_INCREMENTAL;
DROP TASK IF EXISTS O2C_STAGING_REFRESH;
DROP TASK IF EXISTS O2C_DAILY_BUILD;
*/


