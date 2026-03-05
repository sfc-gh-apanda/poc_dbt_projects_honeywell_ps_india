-- ═══════════════════════════════════════════════════════════════════════════════
-- DWS CLIENT REPORTING - SNOWFLAKE TASK SCHEDULING
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Purpose: Automate dbt project execution via Snowflake Tasks
-- Schedule: Daily at 06:00 AM CET (after source data lands)
--
-- Prerequisites:
--   1. Run DWS_LOAD_SAMPLE_DATA.sql
--   2. Run DWS_AUDIT_SETUP.sql
--   3. Deploy dbt project to Snowflake
--
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;
USE DATABASE DWS_EDW;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TASK 1: Daily dbt build (snapshots + models + tests)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD
    WAREHOUSE = DWS_WH_M
    SCHEDULE = 'USING CRON 0 6 * * * Europe/Berlin'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 3600000      -- 1 hour timeout
    COMMENT = 'Daily dbt build for DWS Client Reporting project'
AS
    -- Step 1: Run snapshots (SCD-2 change capture)
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'snapshot --select tag:daily';

    -- Step 2: Build all daily-tagged models and run tests
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'build --select tag:daily';


-- ═══════════════════════════════════════════════════════════════════════════════
-- TASK 2: Weekly full refresh (runs Sunday night)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK DWS_EDW.DWS_AUDIT.DWS_WEEKLY_FULL_REFRESH
    WAREHOUSE = DWS_WH_M
    SCHEDULE = 'USING CRON 0 2 * * 0 Europe/Berlin'     -- Sunday 2 AM
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 7200000      -- 2 hour timeout
    COMMENT = 'Weekly full refresh for DWS Client Reporting'
AS
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'build --full-refresh --select tag:weekly';


-- ═══════════════════════════════════════════════════════════════════════════════
-- TASK 3: Critical reconciliation check (runs after daily build)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK DWS_EDW.DWS_AUDIT.DWS_RECONCILIATION_CHECK
    WAREHOUSE = DWS_WH_S
    AFTER DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD
    COMMENT = 'Run reconciliation tests after daily build'
AS
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'test --select tag:reconciliation';


-- ═══════════════════════════════════════════════════════════════════════════════
-- ERROR HANDLING
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TASK DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD SET
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3;

ALTER TASK DWS_EDW.DWS_AUDIT.DWS_WEEKLY_FULL_REFRESH SET
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3;


-- ═══════════════════════════════════════════════════════════════════════════════
-- ENABLE TASKS (uncomment when ready for production)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ALTER TASK DWS_EDW.DWS_AUDIT.DWS_RECONCILIATION_CHECK RESUME;
-- ALTER TASK DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD RESUME;
-- ALTER TASK DWS_EDW.DWS_AUDIT.DWS_WEEKLY_FULL_REFRESH RESUME;


-- ═══════════════════════════════════════════════════════════════════════════════
-- MONITORING TASK EXECUTION
-- ═══════════════════════════════════════════════════════════════════════════════

-- View task history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 20;

-- View task status
-- SHOW TASKS IN SCHEMA DWS_EDW.DWS_AUDIT;
