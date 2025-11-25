-- ============================================================================
-- Refresh Snowflake Git Repositories with Latest Changes from GitHub
-- ============================================================================
-- Purpose: Fetch latest code changes from GitHub into Snowflake Git repos
-- Run this after pushing new commits to GitHub
-- ============================================================================

-- ============================================================================
-- STEP 1: Check Current Repository Status
-- ============================================================================

USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA GIT_SCHEMA;

-- Show all Git repositories
SHOW GIT REPOSITORIES IN SCHEMA DEPLOYMENT_DB.GIT_SCHEMA;

-- Describe foundation repository
DESCRIBE GIT REPOSITORY dbt_foundation_repo;

-- Describe finance core repository  
DESCRIBE GIT REPOSITORY dbt_finance_core_repo;

-- Show current branches
SHOW GIT BRANCHES IN dbt_foundation_repo;
SHOW GIT BRANCHES IN dbt_finance_core_repo;

-- ============================================================================
-- STEP 2: Fetch Latest Changes from GitHub
-- ============================================================================

-- Fetch foundation repository (pulls latest from main branch)
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;

-- Fetch finance core repository (pulls latest from main branch)
ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;

-- ============================================================================
-- STEP 3: Verify Latest Files Are Available
-- ============================================================================

-- List files in foundation repository root
LS @dbt_foundation_repo/branches/main;

-- List foundation project files (in subdirectory)
LS @dbt_foundation_repo/branches/main/dbt_foundation/;
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/;
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/marts/shared/;
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/;

-- List finance core project files (in subdirectory)
LS @dbt_finance_core_repo/branches/main/dbt_finance_core/;
LS @dbt_finance_core_repo/branches/main/dbt_finance_core/models/;
LS @dbt_finance_core_repo/branches/main/dbt_finance_core/models/marts/finance/;

-- ============================================================================
-- STEP 4: Verify Specific Files Were Updated
-- ============================================================================

-- Check if new files exist in dbt_finance_core
-- These files were just added in the latest commit
SELECT 
    *
FROM 
    DIRECTORY(@dbt_finance_core_repo/branches/main/dbt_finance_core/)
WHERE 
    RELATIVE_PATH IN (
        'profiles.yml',
        'packages.yml',
        'README.md',
        'macros/.gitkeep',
        'seeds/.gitkeep',
        'tests/.gitkeep',
        'snapshots/.gitkeep',
        'analyses/.gitkeep'
    )
ORDER BY 
    RELATIVE_PATH;

-- Check latest commit info
SELECT 
    *
FROM 
    TABLE(
        INFORMATION_SCHEMA.GIT_REPOSITORY_FILES(
            REPOSITORY => 'DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo',
            BRANCH => 'main'
        )
    )
WHERE 
    RELATIVE_PATH LIKE 'dbt_finance_core/%'
ORDER BY 
    LAST_MODIFIED DESC
LIMIT 20;

-- ============================================================================
-- STEP 5: Compare Repository Status Before/After
-- ============================================================================

-- Get the latest commit hash
SELECT 
    SYSTEM$GIT_REPOSITORY_VERSION('DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo', 'main') AS latest_commit;

SELECT 
    SYSTEM$GIT_REPOSITORY_VERSION('DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo', 'main') AS latest_commit;

-- ============================================================================
-- STEP 6: Verify File Contents (Optional)
-- ============================================================================

-- Read a specific file to verify it has latest changes
-- Example: Check dim_customer.sql has the timestamp_ntz fix
SELECT 
    $1 AS file_content
FROM 
    @dbt_foundation_repo/branches/main/dbt_foundation/models/marts/shared/dim_customer.sql;

-- Example: Check dm_fin_ar_aging_simple.sql has the CTE restructure
SELECT 
    $1 AS file_content
FROM 
    @dbt_finance_core_repo/branches/main/dbt_finance_core/models/marts/finance/dm_fin_ar_aging_simple.sql;

-- ============================================================================
-- STEP 7: Refresh Snowflake DBT Projects (If Using Native DBT)
-- ============================================================================

-- If you're using Snowflake's native DBT projects, refresh them
-- This tells Snowflake to reload the code from Git repositories

USE ROLE accountadmin;

-- List existing DBT projects
SHOW DBT PROJECTS;

-- If DBT projects exist, refresh them to pick up Git changes
-- ALTER DBT PROJECT dbt_foundation_project REFRESH;
-- ALTER DBT PROJECT dbt_finance_core_project REFRESH;

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

/*
Issue: Repository not updating

Solution 1: Check GitHub has latest code
- Go to: https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india
- Verify your commits are on main branch
- Look for commits: 
  * c5ed30a (fix: cast current_timestamp)
  * 049a609 (fix: resolve SQL self-referencing)
  * d410f1e (feat: add missing project structure)

Solution 2: Verify credentials are valid
USE ROLE secrets_admin;
SHOW SECRETS IN SCHEMA EDW.INTEGRATION;
-- If GitHub PAT expired, recreate:
-- CREATE OR REPLACE SECRET EDW.INTEGRATION.github_secret
--     TYPE = password
--     USERNAME = 'sfc-gh-apanda'
--     PASSWORD = 'your-new-github-pat';

Solution 3: Check API integration
USE ROLE git_admin;
SHOW API INTEGRATIONS LIKE 'github_api_integration';
DESCRIBE API INTEGRATION github_api_integration;

Solution 4: Verify repository URL is correct
DESCRIBE GIT REPOSITORY dbt_finance_core_repo;
-- If URL is wrong:
-- ALTER GIT REPOSITORY dbt_finance_core_repo
--     SET ORIGIN = 'https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git';

Solution 5: Force re-create repository
DROP GIT REPOSITORY dbt_finance_core_repo;

CREATE OR REPLACE GIT REPOSITORY dbt_finance_core_repo
    API_INTEGRATION = github_api_integration
    GIT_CREDENTIALS = EDW.INTEGRATION.github_secret
    ORIGIN = 'https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git'
    COMMENT = 'DBT Finance Core project - finance domain data marts';

ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;
*/

-- ============================================================================
-- SUCCESS VERIFICATION
-- ============================================================================

/*
✅ If successful, you should see:
1. FETCH commands complete without errors
2. LS commands show all new files (profiles.yml, packages.yml, README.md, etc.)
3. DIRECTORY query shows the 8 new files in dbt_finance_core
4. File content queries show the latest code changes
5. Commit hash matches your latest GitHub commit

Latest commits to verify:
- c5ed30a: fix: cast current_timestamp() to timestamp_ntz
- 049a609: fix: resolve SQL self-referencing alias error  
- d410f1e: feat: add missing project structure files

Next Steps:
1. If using Snowflake DBT UI, refresh the project
2. Re-run dbt build to verify fixes work
3. All contract errors should be resolved
*/

-- ============================================================================
-- End of Refresh Script
-- ============================================================================

