-- ============================================================================
-- Snowflake Git Integration Setup for Honeywell DBT Projects
-- ============================================================================
-- Purpose: Configure GitHub integration for dbt_foundation and dbt_finance_core
-- Prerequisites: GitHub Personal Access Token (PAT) with repo access
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Schema for Integration Secrets
-- ============================================================================

USE ROLE sysadmin;

-- Use existing EDW database
-- CREATE DATABASE IF NOT EXISTS EDW; -- Already exists from data_prep.sql

-- Create integration schema for secrets
CREATE SCHEMA IF NOT EXISTS EDW.INTEGRATION 
    COMMENT = 'Schema for storing integration secrets (GitHub, APIs, etc.)';

-- ============================================================================
-- STEP 2: Create Secrets Admin Role
-- ============================================================================

USE ROLE securityadmin;

-- Create role to manage secrets
CREATE ROLE IF NOT EXISTS secrets_admin 
    COMMENT = 'Role to manage integration secrets';

-- Grant permissions to create and manage secrets
GRANT CREATE SECRET ON SCHEMA EDW.INTEGRATION TO ROLE secrets_admin;

-- Grant database and schema access
GRANT USAGE ON DATABASE EDW TO ROLE secrets_admin;
GRANT USAGE ON SCHEMA EDW.INTEGRATION TO ROLE secrets_admin;

-- Assign to accountadmin (secrets used across account)
GRANT ROLE secrets_admin TO ROLE accountadmin;

-- ============================================================================
-- STEP 3: Create GitHub Secret (Personal Access Token)
-- ============================================================================

USE ROLE secrets_admin;
USE DATABASE EDW;
USE SCHEMA EDW.INTEGRATION;

-- Create secret for GitHub integration
-- IMPORTANT: Replace with your actual GitHub username and PAT token
CREATE OR REPLACE SECRET EDW.INTEGRATION.github_secret
    TYPE = password
    USERNAME = 'sfc-gh-apanda'  -- Your GitHub username
    PASSWORD = 'YOUR_GITHUB_PAT_TOKEN_HERE';  -- ⚠️ REPLACE with your actual GitHub PAT token

-- Grant read access to the secret
GRANT READ ON SECRET EDW.INTEGRATION.github_secret TO ROLE secrets_admin;

-- Verify secret created
SHOW SECRETS IN SCHEMA EDW.INTEGRATION;
SHOW GRANTS ON SECRET EDW.INTEGRATION.github_secret;

-- ============================================================================
-- STEP 4: Create Deployment Database and Schema
-- ============================================================================

USE ROLE sysadmin;

-- Create deployment database for Git repositories
CREATE DATABASE IF NOT EXISTS DEPLOYMENT_DB 
    COMMENT = 'Database for Git repository integrations and deployments';

-- Create schema for Git repositories
CREATE SCHEMA IF NOT EXISTS DEPLOYMENT_DB.GIT_SCHEMA 
    COMMENT = 'Schema for storing Git repository objects';

-- Grant access to secrets_admin
GRANT USAGE ON DATABASE DEPLOYMENT_DB TO ROLE secrets_admin;
GRANT USAGE ON SCHEMA DEPLOYMENT_DB.GIT_SCHEMA TO ROLE secrets_admin;

-- ============================================================================
-- STEP 5: Create Git Admin Role
-- ============================================================================

USE ROLE securityadmin;

-- Create role to manage Git integrations
CREATE ROLE IF NOT EXISTS git_admin 
    COMMENT = 'Role to manage Git integrations and repositories';

-- Grant integration creation permission (requires accountadmin)
USE ROLE accountadmin;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE git_admin;

-- Grant database and schema access
GRANT USAGE ON DATABASE DEPLOYMENT_DB TO ROLE git_admin;
GRANT USAGE ON SCHEMA DEPLOYMENT_DB.GIT_SCHEMA TO ROLE git_admin;

GRANT USAGE ON DATABASE EDW TO ROLE git_admin;
GRANT USAGE ON SCHEMA EDW.INTEGRATION TO ROLE git_admin;

-- Grant Git repository creation permission
USE ROLE securityadmin;
GRANT CREATE GIT REPOSITORY ON SCHEMA DEPLOYMENT_DB.GIT_SCHEMA TO ROLE git_admin;

-- Assign git_admin to accountadmin
GRANT ROLE git_admin TO ROLE accountadmin;

-- Grant access to GitHub secret
GRANT USAGE ON SECRET EDW.INTEGRATION.github_secret TO ROLE git_admin;

-- ============================================================================
-- STEP 6: Create GitHub API Integration
-- ============================================================================

USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA DEPLOYMENT_DB.GIT_SCHEMA;

-- Create API Integration for GitHub
-- IMPORTANT: Update API_ALLOWED_PREFIXES with your GitHub org/user
CREATE OR REPLACE API INTEGRATION github_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-apanda/')  -- Your GitHub user/org
    ALLOWED_AUTHENTICATION_SECRETS = (EDW.INTEGRATION.github_secret)
    ENABLED = TRUE
    COMMENT = 'GitHub API integration for Honeywell DBT projects';

-- Verify integration created
SHOW API INTEGRATIONS LIKE 'github_api_integration';
DESCRIBE API INTEGRATION github_api_integration;

-- ============================================================================
-- STEP 7: Create Git Repository for DBT Foundation Project
-- ============================================================================

-- Create Git repository object for dbt_foundation
-- IMPORTANT: Both projects are in the same repository under implementation/ directory
CREATE OR REPLACE GIT REPOSITORY dbt_foundation_repo
    API_INTEGRATION = github_api_integration
    GIT_CREDENTIALS = EDW.INTEGRATION.github_secret
    ORIGIN = 'https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git'  -- Your actual repo
    COMMENT = 'DBT Foundation project - shared staging and dimensions';

-- Fetch latest code from GitHub
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;

-- Show branches
SHOW GIT BRANCHES IN dbt_foundation_repo;

-- List files in main branch (repository root)
LS @dbt_foundation_repo/branches/main;

-- List dbt_foundation project files (in subdirectory)
LS @dbt_foundation_repo/branches/main/dbt_foundation/;

-- Describe repository
DESCRIBE GIT REPOSITORY dbt_foundation_repo;

-- ============================================================================
-- STEP 8: Create Git Repository for DBT Finance Core Project
-- ============================================================================

-- Create Git repository object for dbt_finance_core
-- IMPORTANT: Same repository, different subdirectory
CREATE OR REPLACE GIT REPOSITORY dbt_finance_core_repo
    API_INTEGRATION = github_api_integration
    GIT_CREDENTIALS = EDW.INTEGRATION.github_secret
    ORIGIN = 'https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git'  -- Same repo as foundation
    COMMENT = 'DBT Finance Core project - finance domain data marts';

-- Fetch latest code from GitHub
ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;

-- Show branches
SHOW GIT BRANCHES IN dbt_finance_core_repo;

-- List files in main branch (repository root)
LS @dbt_finance_core_repo/branches/main;

-- List dbt_finance_core project files (in subdirectory)
LS @dbt_finance_core_repo/branches/main/dbt_finance_core/;

-- Describe repository
DESCRIBE GIT REPOSITORY dbt_finance_core_repo;

-- ============================================================================
-- STEP 9: Verify Setup
-- ============================================================================

-- Show all Git repositories
SHOW GIT REPOSITORIES IN SCHEMA DEPLOYMENT_DB.GIT_SCHEMA;

-- Verify secrets
USE ROLE secrets_admin;
SHOW SECRETS IN SCHEMA EDW.INTEGRATION;

-- Verify roles
USE ROLE securityadmin;
SHOW GRANTS OF ROLE secrets_admin;
SHOW GRANTS OF ROLE git_admin;

-- Verify integrations
USE ROLE git_admin;
SHOW API INTEGRATIONS;

-- ============================================================================
-- STEP 10: Grant Additional Permissions for DBT Execution
-- ============================================================================

USE ROLE accountadmin;

-- Create DBT execution role (if not exists)
CREATE ROLE IF NOT EXISTS dbt_role 
    COMMENT = 'Role for executing DBT projects';

-- Grant access to deployment database
GRANT USAGE ON DATABASE DEPLOYMENT_DB TO ROLE dbt_role;
GRANT USAGE ON SCHEMA DEPLOYMENT_DB.GIT_SCHEMA TO ROLE dbt_role;

-- Grant read access to Git repositories
GRANT READ ON GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo TO ROLE dbt_role;
GRANT READ ON GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo TO ROLE dbt_role;

-- Grant access to EDW database for DBT models
GRANT USAGE ON DATABASE EDW TO ROLE dbt_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE EDW TO ROLE dbt_role;

-- Grant select on source tables
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE dbt_role;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE dbt_role;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_REF TO ROLE dbt_role;

-- Grant create permissions on target schemas
GRANT CREATE TABLE ON SCHEMA EDW.DBT_STAGING TO ROLE dbt_role;
GRANT CREATE VIEW ON SCHEMA EDW.DBT_STAGING TO ROLE dbt_role;

GRANT CREATE TABLE ON SCHEMA EDW.DBT_SHARED TO ROLE dbt_role;
GRANT CREATE VIEW ON SCHEMA EDW.DBT_SHARED TO ROLE dbt_role;

GRANT CREATE TABLE ON SCHEMA EDW.CORP_DM_FIN TO ROLE dbt_role;
GRANT CREATE VIEW ON SCHEMA EDW.CORP_DM_FIN TO ROLE dbt_role;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE dbt_role;

-- Assign dbt_role to your user
GRANT ROLE dbt_role TO USER sfc_gh_apanda;  -- Update with your username

-- ============================================================================
-- STEP 11: Test Repository Access
-- ============================================================================

USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA GIT_SCHEMA;

-- Test foundation repository
SELECT * FROM TABLE(
    RESULT_SCAN(LAST_QUERY_ID())
) LIMIT 10;

-- List foundation models (in subdirectory)
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/;
LS @dbt_foundation_repo/branches/main/dbt_foundation/macros/;

-- Test finance core repository (in subdirectory)
LS @dbt_finance_core_repo/branches/main/dbt_finance_core/models/;

-- ============================================================================
-- STEP 12: Setup Summary and Next Steps
-- ============================================================================

/*
SETUP COMPLETE! ✅

What was created:
1. ✅ EDW.INTEGRATION schema for secrets
2. ✅ secrets_admin role
3. ✅ GitHub secret (EDW.INTEGRATION.github_secret)
4. ✅ DEPLOYMENT_DB database and GIT_SCHEMA
5. ✅ git_admin role
6. ✅ GitHub API integration
7. ✅ dbt_foundation_repo Git repository
8. ✅ dbt_finance_core_repo Git repository
9. ✅ dbt_role for executing DBT projects
10. ✅ All necessary permissions granted

Next Steps:
1. Push your local DBT projects to GitHub repositories
2. Update ORIGIN URLs in this script with your actual GitHub repo URLs
3. Re-run STEP 7 and STEP 8 to point to your repos
4. Create Snowflake DBT Projects using these Git repositories
5. Run dbt commands from Snowflake UI

GitHub Repository (Already Created):
- https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india

Repository Structure:
- Both dbt_foundation and dbt_finance_core are in subdirectories
- /dbt_foundation/ (all foundation files)
- /dbt_finance_core/ (all finance core files)
- Both projects in same repository

Note: Repository already pushed to GitHub with all files!

Verify Setup:
SHOW SECRETS IN SCHEMA EDW.INTEGRATION;
SHOW GIT REPOSITORIES IN SCHEMA DEPLOYMENT_DB.GIT_SCHEMA;
SHOW API INTEGRATIONS;
SHOW GRANTS OF ROLE dbt_role;
*/

-- ============================================================================
-- End of Setup Script
-- ============================================================================

