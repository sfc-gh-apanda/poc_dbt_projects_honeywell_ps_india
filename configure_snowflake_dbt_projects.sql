-- ============================================================================
-- Configure Snowflake DBT Projects with Correct Database Settings
-- ============================================================================
-- Purpose: Fix database configuration to use EDW instead of DEV_DB
-- ============================================================================

USE ROLE accountadmin;

-- ============================================================================
-- STEP 1: Drop Existing Projects (if they exist)
-- ============================================================================

-- Drop existing projects to reconfigure with correct database
DROP DBT PROJECT IF EXISTS dbt_foundation_project;
DROP DBT PROJECT IF EXISTS dbt_finance_core_project;

-- ============================================================================
-- STEP 2: Verify Git Repositories Are Up to Date
-- ============================================================================

USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA GIT_SCHEMA;

-- Fetch latest code
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;
ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;

-- Verify repositories exist
SHOW GIT REPOSITORIES IN SCHEMA DEPLOYMENT_DB.GIT_SCHEMA;

-- ============================================================================
-- STEP 3: Create Foundation DBT Project with EDW Database
-- ============================================================================

USE ROLE accountadmin;

CREATE DBT PROJECT dbt_foundation_project
    USING GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo
    BRANCH = 'main'
    DATABASE = EDW                    -- ✅ IMPORTANT: Use EDW not DEV_DB
    WAREHOUSE = COMPUTE_WH
    ROOT_PATH = 'dbt_foundation'      -- Subdirectory in repo
    SCHEMA = DBT_STAGING;

-- Verify project created
SHOW DBT PROJECTS LIKE 'dbt_foundation_project';
DESCRIBE DBT PROJECT dbt_foundation_project;

-- ============================================================================
-- STEP 4: Create Finance Core DBT Project with EDW Database
-- ============================================================================

CREATE DBT PROJECT dbt_finance_core_project
    USING GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo
    BRANCH = 'main'
    DATABASE = EDW                    -- ✅ IMPORTANT: Use EDW not DEV_DB
    WAREHOUSE = COMPUTE_WH
    ROOT_PATH = 'dbt_finance_core'    -- Subdirectory in repo
    SCHEMA = CORP_DM_FIN;

-- Verify project created
SHOW DBT PROJECTS LIKE 'dbt_finance_core_project';
DESCRIBE DBT PROJECT dbt_finance_core_project;

-- ============================================================================
-- STEP 5: Grant Necessary Permissions
-- ============================================================================

-- Grant access to EDW database and schemas
GRANT USAGE ON DATABASE EDW TO ROLE accountadmin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE EDW TO ROLE accountadmin;

-- Grant SELECT on source tables (foundation needs these)
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE accountadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE accountadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_REF TO ROLE accountadmin;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.CORP_TRAN TO ROLE accountadmin;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.CORP_MASTER TO ROLE accountadmin;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.CORP_REF TO ROLE accountadmin;

-- Grant CREATE permissions on target schemas (foundation creates these)
GRANT CREATE TABLE ON SCHEMA EDW.DBT_STAGING TO ROLE accountadmin;
GRANT CREATE VIEW ON SCHEMA EDW.DBT_STAGING TO ROLE accountadmin;
GRANT CREATE TABLE ON SCHEMA EDW.DBT_SHARED TO ROLE accountadmin;
GRANT CREATE VIEW ON SCHEMA EDW.DBT_SHARED TO ROLE accountadmin;

-- Grant CREATE permissions on finance core schema
GRANT CREATE TABLE ON SCHEMA EDW.CORP_DM_FIN TO ROLE accountadmin;
GRANT CREATE VIEW ON SCHEMA EDW.CORP_DM_FIN TO ROLE accountadmin;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE accountadmin;

-- ============================================================================
-- STEP 6: Build Foundation Project FIRST
-- ============================================================================

-- Build foundation to create source tables that finance_core depends on
EXECUTE DBT PROJECT dbt_foundation_project
    COMMAND = 'build'
    WAREHOUSE = COMPUTE_WH;

-- Check execution status
SHOW DBT EXECUTIONS FOR PROJECT dbt_foundation_project 
    ORDER BY created_time DESC 
    LIMIT 10;

-- Verify foundation tables were created
SHOW TABLES IN SCHEMA EDW.DBT_STAGING;
SHOW TABLES IN SCHEMA EDW.DBT_SHARED;

-- Verify specific tables exist
SELECT COUNT(*) FROM EDW.DBT_STAGING.STG_AR_INVOICE;
SELECT COUNT(*) FROM EDW.DBT_SHARED.DIM_CUSTOMER;
SELECT COUNT(*) FROM EDW.DBT_SHARED.DIM_FISCAL_CALENDAR;

-- ============================================================================
-- STEP 7: Build Finance Core Project SECOND
-- ============================================================================

-- Now build finance core (depends on foundation tables)
EXECUTE DBT PROJECT dbt_finance_core_project
    COMMAND = 'build'
    WAREHOUSE = COMPUTE_WH;

-- Check execution status
SHOW DBT EXECUTIONS FOR PROJECT dbt_finance_core_project 
    ORDER BY created_time DESC 
    LIMIT 10;

-- Verify finance core table was created
SHOW TABLES IN SCHEMA EDW.CORP_DM_FIN;

-- Verify data
SELECT COUNT(*) FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE;
SELECT * FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE LIMIT 10;

-- ============================================================================
-- STEP 8: Verify Final Configuration
-- ============================================================================

-- Show all DBT projects
SHOW DBT PROJECTS;

-- Show project details
DESCRIBE DBT PROJECT dbt_foundation_project;
DESCRIBE DBT PROJECT dbt_finance_core_project;

-- List recent executions
SELECT 
    project_name,
    command,
    state,
    created_time,
    completed_time,
    error_message
FROM 
    TABLE(INFORMATION_SCHEMA.DBT_EXECUTIONS())
ORDER BY 
    created_time DESC
LIMIT 20;

-- ============================================================================
-- SUCCESS CRITERIA
-- ============================================================================

/*
✅ After running this script, you should have:

1. DBT Projects Created:
   - dbt_foundation_project (DATABASE=EDW, SCHEMA=DBT_STAGING)
   - dbt_finance_core_project (DATABASE=EDW, SCHEMA=CORP_DM_FIN)

2. Foundation Tables Created:
   - EDW.DBT_STAGING.STG_AR_INVOICE
   - EDW.DBT_SHARED.DIM_CUSTOMER
   - EDW.DBT_SHARED.DIM_FISCAL_CALENDAR

3. Finance Core Table Created:
   - EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE

4. No Errors:
   - No "database does not exist" errors
   - No "object does not exist" errors
   - All builds complete successfully
*/

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

-- If you still get database errors, verify EDW database exists:
SHOW DATABASES LIKE 'EDW';

-- If EDW doesn't exist, create it:
-- CREATE DATABASE IF NOT EXISTS EDW;

-- If schemas don't exist, create them:
-- CREATE SCHEMA IF NOT EXISTS EDW.DBT_STAGING;
-- CREATE SCHEMA IF NOT EXISTS EDW.DBT_SHARED;
-- CREATE SCHEMA IF NOT EXISTS EDW.CORP_DM_FIN;

-- Check if source tables exist:
SHOW TABLES IN SCHEMA EDW.CORP_TRAN;
SHOW TABLES IN SCHEMA EDW.CORP_MASTER;
SHOW TABLES IN SCHEMA EDW.CORP_REF;

-- ============================================================================
-- End of Configuration Script
-- ============================================================================

