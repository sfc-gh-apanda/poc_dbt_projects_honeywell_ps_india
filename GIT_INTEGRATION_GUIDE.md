# Snowflake Git Integration Guide for Honeywell DBT Projects

## Overview

This guide shows how to integrate your DBT projects with GitHub in Snowflake, enabling:
- ✅ Version-controlled DBT code
- ✅ Direct deployment from GitHub to Snowflake
- ✅ Automated sync and updates
- ✅ Team collaboration via Git

---

## Prerequisites

### 1. GitHub Personal Access Token (PAT)

**Create a GitHub PAT:**
1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Give it a name: `snowflake-dbt-integration`
4. Select scopes:
   - ✅ `repo` (Full control of private repositories)
   - ✅ `read:org` (if using organization repos)
5. Click "Generate token"
6. **Copy the token** (you won't see it again!)
   - Example: `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

### 2. GitHub Repositories

Create two GitHub repositories:
```
https://github.com/<your-username>/honeywell-dbt-foundation
https://github.com/<your-username>/honeywell-dbt-finance-core
```

---

## Quick Start (3 Steps)

### Step 1: Update the Script with Your Details

Open `snowflake_git_integration_setup.sql` and update:

**Line 44 - Your GitHub username:**
```sql
CREATE OR REPLACE SECRET EDW.INTEGRATION.github_secret
    TYPE = password
    USERNAME = 'YOUR_GITHUB_USERNAME'  -- ← Change this
    PASSWORD = 'YOUR_GITHUB_PAT_TOKEN';  -- ← Change this
```

**Line 103 - Your GitHub organization/user:**
```sql
CREATE OR REPLACE API INTEGRATION github_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/YOUR_USERNAME/')  -- ← Change this
```

**Line 119 - Foundation repository URL:**
```sql
CREATE OR REPLACE GIT REPOSITORY dbt_foundation_repo
    ORIGIN = 'https://github.com/YOUR_USERNAME/honeywell-dbt-foundation.git'  -- ← Change this
```

**Line 141 - Finance Core repository URL:**
```sql
CREATE OR REPLACE GIT REPOSITORY dbt_finance_core_repo
    ORIGIN = 'https://github.com/YOUR_USERNAME/honeywell-dbt-finance-core.git'  -- ← Change this
```

**Line 197 - Your Snowflake username:**
```sql
GRANT ROLE dbt_role TO USER YOUR_SNOWFLAKE_USERNAME;  -- ← Change this
```

### Step 2: Push Code to GitHub

```bash
# Navigate to foundation project
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/dbt_foundation

# Initialize Git and push
git init
git add .
git commit -m "Initial foundation project"
git remote add origin https://github.com/YOUR_USERNAME/honeywell-dbt-foundation.git
git push -u origin main

# Navigate to finance core project
cd ../dbt_finance_core

# Initialize Git and push
git init
git add .
git commit -m "Initial finance core project"
git remote add origin https://github.com/YOUR_USERNAME/honeywell-dbt-finance-core.git
git push -u origin main
```

### Step 3: Run Setup Script in Snowflake

1. Open Snowflake Web UI
2. Create new worksheet
3. Copy entire contents of `snowflake_git_integration_setup.sql`
4. Paste into worksheet
5. Click "Run All" (or run section by section)

---

## Mapping: Your GitLab Script → GitHub Script

| Your GitLab Script | Equivalent GitHub Script | Changes Made |
|-------------------|-------------------------|--------------|
| `common_db.integration` | `EDW.INTEGRATION` | Using existing EDW database |
| `gitlab_secret` | `github_secret` | Changed to GitHub |
| `gitlab_api_integration` | `github_api_integration` | GitHub provider |
| GitLab dedicated URL | `https://github.com/` | Standard GitHub |
| `gitlab_repo` | `dbt_foundation_repo` & `dbt_finance_core_repo` | Two separate repos |
| Single repo | Two repos | Foundation + Finance Core |

---

## What Gets Created

### Databases & Schemas
```
EDW (existing)
└── INTEGRATION (new)
    └── github_secret

DEPLOYMENT_DB (new)
└── GIT_SCHEMA (new)
    ├── dbt_foundation_repo
    └── dbt_finance_core_repo
```

### Roles
```
secrets_admin
├── Can create and manage secrets
└── Can access EDW.INTEGRATION

git_admin
├── Can create Git integrations
├── Can create Git repositories
└── Can access both databases

dbt_role
├── Can read from Git repositories
├── Can execute DBT models
└── Can create tables/views in target schemas
```

### Integrations
```
github_api_integration
├── Provider: git_https_api
├── Allowed: https://github.com/YOUR_USERNAME/
└── Auth: EDW.INTEGRATION.github_secret
```

### Git Repositories
```
dbt_foundation_repo
├── Origin: https://github.com/YOUR_USERNAME/honeywell-dbt-foundation.git
└── Credentials: github_secret

dbt_finance_core_repo
├── Origin: https://github.com/YOUR_USERNAME/honeywell-dbt-finance-core.git
└── Credentials: github_secret
```

---

## Verification Commands

### After Running Setup, Verify:

```sql
-- 1. Check secrets
USE ROLE secrets_admin;
SHOW SECRETS IN SCHEMA EDW.INTEGRATION;
-- Expected: github_secret

-- 2. Check API integration
USE ROLE git_admin;
SHOW API INTEGRATIONS;
-- Expected: github_api_integration

-- 3. Check Git repositories
SHOW GIT REPOSITORIES IN SCHEMA DEPLOYMENT_DB.GIT_SCHEMA;
-- Expected: dbt_foundation_repo, dbt_finance_core_repo

-- 4. List files from foundation repo
LS @DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo/branches/main;
-- Expected: dbt_project.yml, models/, macros/, etc.

-- 5. List files from finance core repo
LS @DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo/branches/main;
-- Expected: dbt_project.yml, models/, dependencies.yml

-- 6. Verify branches
SHOW GIT BRANCHES IN DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo;
SHOW GIT BRANCHES IN DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo;
-- Expected: main branch

-- 7. Check roles and grants
USE ROLE securityadmin;
SHOW GRANTS OF ROLE git_admin;
SHOW GRANTS OF ROLE dbt_role;
```

---

## Using Git Repositories in Snowflake

### Fetch Latest Changes
```sql
USE ROLE git_admin;

-- Fetch updates from GitHub
ALTER GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo FETCH;
ALTER GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo FETCH;
```

### List Files
```sql
-- List all files in foundation repo
LS @DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo/branches/main;

-- List specific folder
LS @DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo/branches/main/models/;
LS @DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo/branches/main/models/staging/;
```

### Execute SQL from Repository
```sql
-- Execute a SQL file directly from Git
EXECUTE IMMEDIATE FROM @DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo/branches/main/models/staging/stg_ar/stg_ar_invoice.sql;
```

---

## Creating Snowflake DBT Project from Git

### Option 1: Using Snowflake UI

1. Go to **Projects** → **+ Project** → **DBT Project**
2. Select **Git Repository** as source
3. Choose repository:
   - Database: `DEPLOYMENT_DB`
   - Schema: `GIT_SCHEMA`
   - Repository: `dbt_foundation_repo`
   - Branch: `main`
4. Configure connection:
   - Database: `EDW`
   - Warehouse: `COMPUTE_WH`
   - Schema: `DBT_STAGING`
5. Click **Create**

### Option 2: Using SQL

```sql
USE ROLE accountadmin;

-- Create DBT project from Git repository
CREATE DBT PROJECT dbt_foundation_project
    USING GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo
    BRANCH = 'main'
    DATABASE = EDW
    WAREHOUSE = COMPUTE_WH
    SCHEMA = DBT_STAGING;

-- Repeat for finance core
CREATE DBT PROJECT dbt_finance_core_project
    USING GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo
    BRANCH = 'main'
    DATABASE = EDW
    WAREHOUSE = COMPUTE_WH
    SCHEMA = CORP_DM_FIN;
```

---

## Workflow: Develop → Commit → Deploy

### 1. Local Development
```bash
# Make changes locally
cd dbt_foundation
vim models/staging/stg_ar/stg_ar_invoice.sql

# Test locally (optional)
dbt run --select stg_ar_invoice
```

### 2. Commit to GitHub
```bash
git add .
git commit -m "Updated AR invoice staging model"
git push origin main
```

### 3. Deploy in Snowflake
```sql
-- Fetch latest changes
USE ROLE git_admin;
ALTER GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo FETCH;

-- In DBT project, click "Refresh" or run:
USE ROLE dbt_role;
-- Snowflake DBT project will auto-detect changes
-- Run dbt commands in Snowflake UI
```

---

## Troubleshooting

### Issue: "Invalid credentials"
```sql
-- Recreate secret with correct GitHub PAT
USE ROLE secrets_admin;
CREATE OR REPLACE SECRET EDW.INTEGRATION.github_secret
    TYPE = password
    USERNAME = 'your-github-username'
    PASSWORD = 'your-new-github-pat';
```

### Issue: "Repository not found"
```sql
-- Verify repository URL
DESCRIBE GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo;

-- Update repository URL
ALTER GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo
    SET ORIGIN = 'https://github.com/correct-username/honeywell-dbt-foundation.git';
```

### Issue: "Permission denied"
```sql
-- Check PAT token has correct permissions on GitHub:
-- - repo (full control)
-- - read:org (if using org)

-- Verify secret grants
SHOW GRANTS ON SECRET EDW.INTEGRATION.github_secret;
```

### Issue: "Cannot fetch repository"
```sql
-- Check GitHub repository exists and is accessible
-- Verify PAT token is valid (they expire!)
-- Try manual fetch with error details:
ALTER GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo FETCH;
```

---

## Security Best Practices

### 1. Rotate PAT Tokens Regularly
```sql
-- Update secret when PAT changes
USE ROLE secrets_admin;
ALTER SECRET EDW.INTEGRATION.github_secret 
    SET PASSWORD = 'new-github-pat-token';
```

### 2. Use Least Privilege
```sql
-- Only grant necessary permissions
-- Don't give dbt_role access to secrets
-- Keep secrets_admin and git_admin separate
```

### 3. Audit Access
```sql
-- Regular audits
SHOW GRANTS ON SECRET EDW.INTEGRATION.github_secret;
SHOW GRANTS OF ROLE git_admin;
SHOW GRANTS OF ROLE dbt_role;
```

### 4. Use Private Repositories
- Always use private GitHub repositories for production code
- Never commit secrets or credentials to Git
- Use `.gitignore` to exclude sensitive files

---

## Summary

### What You Configured
1. ✅ GitHub secret storage in Snowflake
2. ✅ API integration with GitHub
3. ✅ Two Git repository objects (foundation + finance)
4. ✅ Roles for secrets, Git, and DBT execution
5. ✅ All necessary permissions

### What You Can Do Now
1. ✅ Version control DBT code in GitHub
2. ✅ Deploy directly from GitHub to Snowflake
3. ✅ Automatic sync with `ALTER ... FETCH`
4. ✅ Team collaboration via Git workflows
5. ✅ Run DBT from Snowflake UI with Git-backed code

### Next Steps
1. Push your DBT code to GitHub
2. Run the setup script in Snowflake
3. Verify repositories are accessible
4. Create Snowflake DBT projects from Git
5. Start developing with Git-backed workflow

---

**You're now ready to use Git-integrated DBT in Snowflake!** 🚀

