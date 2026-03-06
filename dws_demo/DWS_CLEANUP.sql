-- ═══════════════════════════════════════════════════════════════════════════════
-- DWS CLIENT REPORTING - COMPLETE CLEANUP SCRIPT
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Purpose: Drop everything created by the DWS demo project for a fresh start
-- Order:   Tasks → Monitoring → Audit → dbt schemas → Source schemas → Databases → Roles
--
-- WARNING: This is DESTRUCTIVE. All data will be lost.
--          Review before running. Uncomment the sections you want to execute.
--
-- After cleanup, re-run:
--   1. DWS_LOAD_SAMPLE_DATA.sql
--   2. DWS_AUDIT_SETUP.sql
--   3. dbt deps → dbt snapshot → dbt build
--   4. DWS_MONITORING_DASHBOARD.sql
--   5. DWS_SCHEDULING.sql (optional)
-- ═══════════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: SUSPEND & DROP SNOWFLAKE TASKS
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;

ALTER TASK IF EXISTS DWS_EDW.DWS_AUDIT.DWS_RECONCILIATION_CHECK SUSPEND;
ALTER TASK IF EXISTS DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD SUSPEND;
ALTER TASK IF EXISTS DWS_EDW.DWS_AUDIT.DWS_WEEKLY_FULL_REFRESH SUSPEND;

DROP TASK IF EXISTS DWS_EDW.DWS_AUDIT.DWS_RECONCILIATION_CHECK;
DROP TASK IF EXISTS DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD;
DROP TASK IF EXISTS DWS_EDW.DWS_AUDIT.DWS_WEEKLY_FULL_REFRESH;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: DROP MONITORING SCHEMA
-- ═══════════════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS DWS_EDW.DWS_MONITORING CASCADE;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: DROP AUDIT SCHEMA
-- ═══════════════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS DWS_EDW.DWS_AUDIT CASCADE;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: DROP DBT-CREATED SCHEMAS (prod)
-- ═══════════════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS DWS_EDW.DWS_CLIENT_REPORTING CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_CLIENT_REPORTING_STAGING CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_CLIENT_REPORTING_DIMENSIONS CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_CLIENT_REPORTING_CORE CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_CLIENT_REPORTING_EVENTS CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_CLIENT_REPORTING_AGGREGATES CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_SNAPSHOTS CASCADE;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: DROP SOURCE SCHEMAS (prod)
-- ═══════════════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS DWS_EDW.DWS_TRAN CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_MASTER CASCADE;
DROP SCHEMA IF EXISTS DWS_EDW.DWS_REF CASCADE;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: DROP DEV & TEST DATABASES
-- ═══════════════════════════════════════════════════════════════════════════════

DROP DATABASE IF EXISTS DWS_EDWDEV;
DROP DATABASE IF EXISTS DWS_EDWTEST;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 7: DROP PROD DATABASE
-- ═══════════════════════════════════════════════════════════════════════════════

DROP DATABASE IF EXISTS DWS_EDW;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 8: DROP ROLES
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SECURITYADMIN;

DROP ROLE IF EXISTS DWS_DEVELOPER;
DROP ROLE IF EXISTS DWS_TESTER;
DROP ROLE IF EXISTS DWS_PROD;


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;

SHOW DATABASES LIKE 'DWS%';

SELECT 'CLEANUP COMPLETE - Ready for fresh setup' AS status;
