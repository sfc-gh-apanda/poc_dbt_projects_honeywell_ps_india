-- ═══════════════════════════════════════════════════════════════════════════════
-- GITHUB ACTIONS CI/CD SETUP - OIDC AUTHENTICATION
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Configure Snowflake for GitHub Actions with OIDC (OpenID Connect)
-- Authentication Type: OIDC (No private keys needed)
-- Snowflake CLI Version Required: 3.11.0+
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: Create CI/CD Role with Limited Privileges (Security Best Practice)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE ROLE IF NOT EXISTS GITHUB_CICD_ROLE
  COMMENT = 'Role for GitHub Actions CI/CD automation';

-- Grant database and warehouse usage
GRANT USAGE ON DATABASE EDW TO ROLE GITHUB_CICD_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE GITHUB_CICD_ROLE;

-- Grant schema privileges for O2C Enhanced project
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_STAGING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_DIMENSIONS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_EVENTS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_PARTITIONED TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_AGGREGATES TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_AUDIT TO ROLE GITHUB_CICD_ROLE;

-- Grant privileges on existing tables (if any)
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_ENHANCED_STAGING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_ENHANCED_DIMENSIONS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_ENHANCED_EVENTS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_ENHANCED_PARTITIONED TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_ENHANCED_AGGREGATES TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA EDW.O2C_AUDIT TO ROLE GITHUB_CICD_ROLE;

-- Grant privileges on existing views
GRANT ALL ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON ALL VIEWS IN SCHEMA EDW.O2C_AUDIT TO ROLE GITHUB_CICD_ROLE;

-- Grant privileges on future objects (important for dbt)
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_STAGING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_DIMENSIONS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_EVENTS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_PARTITIONED TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_AGGREGATES TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_AUDIT TO ROLE GITHUB_CICD_ROLE;

GRANT ALL ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA EDW.O2C_AUDIT TO ROLE GITHUB_CICD_ROLE;

-- Grant access to source schemas (read-only)
GRANT USAGE ON SCHEMA EDW.CORP_TRAN TO ROLE GITHUB_CICD_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_MASTER TO ROLE GITHUB_CICD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE GITHUB_CICD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE GITHUB_CICD_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.CORP_TRAN TO ROLE GITHUB_CICD_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.CORP_MASTER TO ROLE GITHUB_CICD_ROLE;

SELECT '✅ Step 1 Complete: GITHUB_CICD_ROLE created and configured' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: Generate OIDC Subject String
-- ═══════════════════════════════════════════════════════════════════════════════
-- Run this in your terminal (requires gh CLI):
-- gh repo view sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india --json nameWithOwner | jq -r '"repo:\(.nameWithOwner):environment:production"'
--
-- Expected output: repo:sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india:environment:production
--
-- Or manually construct: repo:<org>/<repo>:environment:<environment-name>
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: Create Service User with OIDC Authentication
-- ═══════════════════════════════════════════════════════════════════════════════

-- IMPORTANT: Replace the SUBJECT value with your actual repository subject from Step 2!
CREATE USER IF NOT EXISTS github_actions_service_user
  TYPE = SERVICE
  WORKLOAD_IDENTITY = (
    TYPE = OIDC
    ISSUER = 'https://token.actions.githubusercontent.com'
    SUBJECT = 'repo:sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india:environment:production'
  )
  DEFAULT_ROLE = GITHUB_CICD_ROLE
  COMMENT = 'Service user for GitHub Actions CI/CD with OIDC authentication';

-- Grant the CI/CD role to the service user
GRANT ROLE GITHUB_CICD_ROLE TO USER github_actions_service_user;

SELECT '✅ Step 3 Complete: Service user created with OIDC authentication' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: Verify Configuration
-- ═══════════════════════════════════════════════════════════════════════════════

-- Check service user configuration
DESC USER github_actions_service_user;

-- Verify role grants
SHOW GRANTS TO ROLE GITHUB_CICD_ROLE;

-- Verify user has the role
SHOW GRANTS TO USER github_actions_service_user;

SELECT '✅ Step 4 Complete: Configuration verified' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: Create Audit Log for CI/CD Runs (Optional but Recommended)
-- ═══════════════════════════════════════════════════════════════════════════════

USE SCHEMA EDW.O2C_AUDIT;

CREATE TABLE IF NOT EXISTS GITHUB_ACTIONS_RUN_LOG (
    run_id VARCHAR(200) PRIMARY KEY,
    workflow_name VARCHAR(200),
    run_number NUMBER,
    triggered_by VARCHAR(100),
    branch VARCHAR(100),
    commit_sha VARCHAR(200),
    run_started_at TIMESTAMP_NTZ,
    run_completed_at TIMESTAMP_NTZ,
    run_duration_seconds NUMBER,
    run_status VARCHAR(50),
    job_name VARCHAR(200),
    models_deployed NUMBER,
    tests_passed NUMBER,
    tests_failed NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT '✅ Step 5 Complete: Audit table created for GitHub Actions tracking' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERY: Test if user can access required objects
-- ═══════════════════════════════════════════════════════════════════════════════

-- Run this as github_actions_service_user (if you want to test manually)
-- USE ROLE GITHUB_CICD_ROLE;
-- SHOW SCHEMAS IN DATABASE EDW;
-- SHOW TABLES IN SCHEMA EDW.O2C_ENHANCED_CORE;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SUMMARY
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT '═══════════════════════════════════════════════════════════' AS "═══";
SELECT '✅ GITHUB ACTIONS OIDC SETUP COMPLETE!' AS status;
SELECT '═══════════════════════════════════════════════════════════' AS "═══";

SELECT 
    'Service User' AS component,
    'github_actions_service_user' AS value,
    '✅ Created' AS status
UNION ALL
SELECT 
    'Authentication Type',
    'OIDC (OpenID Connect)',
    '✅ Configured'
UNION ALL
SELECT 
    'Role',
    'GITHUB_CICD_ROLE',
    '✅ Granted'
UNION ALL
SELECT 
    'Issuer',
    'https://token.actions.githubusercontent.com',
    '✅ Configured'
UNION ALL
SELECT 
    'Subject',
    'repo:sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india:environment:production',
    '⚠️ Verify this matches your repo';

-- ═══════════════════════════════════════════════════════════════════════════════
-- NEXT STEPS:
-- 1. ✅ Go to GitHub repo → Settings → Environments
-- 2. ✅ Create environment named "production"
-- 3. ✅ Go to Settings → Secrets and variables → Actions
-- 4. ✅ Add secret: SNOWFLAKE_ACCOUNT = your-account-identifier
-- 5. ✅ Commit and push the workflow file (.github/workflows/dbt_o2c_deploy.yml)
-- 6. ✅ Test with: Actions tab → Run workflow manually
-- 7. ✅ Push a change to main branch to trigger automatic deployment
-- ═══════════════════════════════════════════════════════════════════════════════

