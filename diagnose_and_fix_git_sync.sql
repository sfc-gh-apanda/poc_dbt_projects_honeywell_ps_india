-- ============================================================================
-- Diagnose and Fix Git Repository Sync Issues in Snowflake
-- ============================================================================
-- Problem: Files exist in GitHub but not visible in Snowflake Git repo
-- ============================================================================

USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA GIT_SCHEMA;

-- ============================================================================
-- STEP 1: Check Repository Configuration
-- ============================================================================

-- Show repository details
DESCRIBE GIT REPOSITORY dbt_foundation_repo;

-- Expected output should show:
-- ORIGIN: https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git
-- API_INTEGRATION: github_api_integration

-- Check last fetch time
SELECT 
    name,
    database_name,
    schema_name,
    origin,
    last_fetched_at
FROM 
    TABLE(INFORMATION_SCHEMA.GIT_REPOSITORIES())
WHERE 
    name IN ('dbt_foundation_repo', 'dbt_finance_core_repo');

-- ============================================================================
-- STEP 2: Test Repository Access
-- ============================================================================

-- Try to list root directory
LS @dbt_foundation_repo/branches/main;

-- If you get an error about authentication or access, the secret may be invalid
-- If you see files, continue to next step

-- ============================================================================
-- STEP 3: FORCE FETCH (Pull Latest from GitHub)
-- ============================================================================

-- This is the KEY command - forces Snowflake to pull from GitHub
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;

-- Wait 5 seconds then verify
SELECT SYSTEM$WAIT(5);

ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;

SELECT SYSTEM$WAIT(5);

-- ============================================================================
-- STEP 4: Verify Files Are Now Visible
-- ============================================================================

-- List root level - should show dbt_foundation/ directory
LS @dbt_foundation_repo/branches/main;

-- List dbt_foundation directory contents
LS @dbt_foundation_repo/branches/main/dbt_foundation/;

-- List models directory
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/;

-- List staging directory
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/;

-- List stg_ar directory (THIS IS WHAT WAS MISSING)
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/stg_ar/;

-- Expected output:
-- _stg_ar.yml
-- stg_ar_invoice.sql

-- ============================================================================
-- STEP 5: Verify Specific Files Exist
-- ============================================================================

-- Check if _stg_ar.yml exists (the new file we added)
SELECT 
    relative_path,
    size,
    last_modified
FROM 
    DIRECTORY(@dbt_foundation_repo/branches/main/dbt_foundation/)
WHERE 
    relative_path = 'models/staging/stg_ar/_stg_ar.yml';

-- Check if dbt_project.yml was updated (staging access changed to public)
SELECT 
    relative_path,
    size,
    last_modified
FROM 
    DIRECTORY(@dbt_foundation_repo/branches/main/dbt_foundation/)
WHERE 
    relative_path = 'dbt_project.yml';

-- ============================================================================
-- STEP 6: Verify Latest Commit
-- ============================================================================

-- Get current commit hash
SELECT SYSTEM$GIT_REPOSITORY_VERSION(
    'DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo', 
    'main'
) AS current_commit_hash;

-- Expected: Should match or be after commit c0e01d1
-- c0e01d1: fix: enable cross-project access to staging models

-- ============================================================================
-- STEP 7: Read File Contents to Verify Changes
-- ============================================================================

-- Read _stg_ar.yml to verify it exists
SELECT $1 AS file_content
FROM @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/stg_ar/_stg_ar.yml
LIMIT 10;

-- Read dbt_project.yml and check for 'access: public'
SELECT $1 AS file_content
FROM @dbt_foundation_repo/branches/main/dbt_foundation/dbt_project.yml
WHERE $1 LIKE '%staging:%' 
   OR $1 LIKE '%+access:%';

-- Should show: +access: public (not private)

-- ============================================================================
-- STEP 8: If Still No Results - Troubleshoot
-- ============================================================================

-- Issue 1: Check if secret is valid
USE ROLE secrets_admin;
SHOW SECRETS IN SCHEMA EDW.INTEGRATION;

-- If github_secret doesn't exist or is expired, recreate:
/*
CREATE OR REPLACE SECRET EDW.INTEGRATION.github_secret
    TYPE = password
    USERNAME = 'sfc-gh-apanda'
    PASSWORD = 'YOUR_NEW_GITHUB_PAT';
*/

-- Issue 2: Check API integration
USE ROLE git_admin;
SHOW API INTEGRATIONS LIKE 'github_api_integration';

-- Issue 3: Verify repository URL is correct
DESCRIBE GIT REPOSITORY dbt_foundation_repo;

-- If URL is wrong, fix it:
/*
ALTER GIT REPOSITORY dbt_foundation_repo
    SET ORIGIN = 'https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git';
*/

-- Issue 4: Check branch exists
SHOW GIT BRANCHES IN dbt_foundation_repo;

-- Should show 'main' branch

-- ============================================================================
-- STEP 9: Nuclear Option - Recreate Repository
-- ============================================================================

-- Only do this if FETCH still doesn't work
/*
USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA GIT_SCHEMA;

-- Drop and recreate
DROP GIT REPOSITORY IF EXISTS dbt_foundation_repo;

CREATE GIT REPOSITORY dbt_foundation_repo
    API_INTEGRATION = github_api_integration
    GIT_CREDENTIALS = EDW.INTEGRATION.github_secret
    ORIGIN = 'https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india.git'
    COMMENT = 'DBT Foundation project - shared staging and dimensions';

-- Fetch immediately
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;

-- Verify
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/stg_ar/;
*/

-- ============================================================================
-- STEP 10: Verify Finance Core Repository
-- ============================================================================

-- Same process for finance core
LS @dbt_finance_core_repo/branches/main/dbt_finance_core/;

-- Should show all the new files:
-- README.md
-- profiles.yml
-- packages.yml
-- macros/
-- seeds/
-- etc.

-- ============================================================================
-- SUCCESS CRITERIA
-- ============================================================================

/*
✅ After FETCH, you should see:

1. LS @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/stg_ar/
   Results:
   - _stg_ar.yml
   - stg_ar_invoice.sql

2. Reading dbt_project.yml shows: +access: public

3. LS @dbt_finance_core_repo/branches/main/dbt_finance_core/
   Results:
   - README.md
   - profiles.yml
   - packages.yml
   - models/
   - macros/
   - seeds/
   - tests/
   - snapshots/
   - analyses/

4. Commit hash matches latest: a216a11 or later

If all above pass, Git sync is working correctly!
*/

-- ============================================================================
-- NEXT STEP: Build Projects
-- ============================================================================

/*
Once files are visible in Snowflake:

1. Build foundation FIRST:
   EXECUTE DBT PROJECT dbt_foundation_project
       COMMAND = 'build'
       WAREHOUSE = COMPUTE_WH;

2. Build finance core SECOND:
   EXECUTE DBT PROJECT dbt_finance_core_project
       COMMAND = 'build'
       WAREHOUSE = COMPUTE_WH;

The cross-project reference error should be resolved!
*/

-- ============================================================================
-- End of Diagnostic Script
-- ============================================================================

