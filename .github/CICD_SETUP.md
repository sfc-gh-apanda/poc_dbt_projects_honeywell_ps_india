# GitHub Actions CI/CD Setup Guide

**Purpose:** Step-by-step guide to configure GitHub Actions with OIDC authentication for O2C Enhanced  
**Created:** January 2025  
**Authentication:** OIDC (OpenID Connect) - No secrets storage required!

---

## 📋 Prerequisites

- [ ] GitHub repository admin access
- [ ] Snowflake ACCOUNTADMIN role access
- [ ] `gh` CLI installed (optional, for subject generation)

---

## 🚀 Quick Setup (3 Steps)

### Step 1: Configure Snowflake (Run Once)

Connect to Snowflake and execute:

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- GITHUB ACTIONS OIDC SETUP - Run this in Snowflake
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1.1: Create CI/CD Role with Limited Privileges
-- ─────────────────────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS GITHUB_CICD_ROLE
  COMMENT = 'Role for GitHub Actions CI/CD pipeline';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE GITHUB_CICD_ROLE;

-- Grant database access
GRANT USAGE ON DATABASE EDW TO ROLE GITHUB_CICD_ROLE;

-- Grant schema access for O2C Enhanced
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_STAGING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_DIMENSIONS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_EVENTS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_PARTITIONED TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_AGGREGATES TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_AUDIT TO ROLE GITHUB_CICD_ROLE;

-- Grant source schema access (read only)
GRANT USAGE ON SCHEMA EDW.CORP_TRAN TO ROLE GITHUB_CICD_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_MASTER TO ROLE GITHUB_CICD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE GITHUB_CICD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE GITHUB_CICD_ROLE;

-- Grant future privileges
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_STAGING TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_DIMENSIONS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_EVENTS TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_PARTITIONED TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA EDW.O2C_ENHANCED_AGGREGATES TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE GITHUB_CICD_ROLE;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1.2: Create Service User with OIDC Authentication
-- ─────────────────────────────────────────────────────────────────────────────
-- IMPORTANT: Replace <your-org> and <your-repo> with actual values!
-- Format: repo:<owner>/<repo>:environment:<environment>

CREATE USER IF NOT EXISTS github_actions_service_user
  TYPE = SERVICE
  DEFAULT_ROLE = GITHUB_CICD_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  WORKLOAD_IDENTITY = (
    TYPE = OIDC
    ISSUER = 'https://token.actions.githubusercontent.com'
    SUBJECT = 'repo:sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india:environment:production'
  )
  COMMENT = 'Service user for GitHub Actions CI/CD with OIDC authentication';

-- Grant role to user
GRANT ROLE GITHUB_CICD_ROLE TO USER github_actions_service_user;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1.3: Verify Setup
-- ─────────────────────────────────────────────────────────────────────────────
-- Check user was created
SHOW USERS LIKE 'github_actions_service_user';

-- Check role grants
SHOW GRANTS TO USER github_actions_service_user;
SHOW GRANTS TO ROLE GITHUB_CICD_ROLE;

SELECT '✅ Snowflake OIDC setup complete!' AS status;
```

---

### Step 2: Configure GitHub

#### 2.1 Create Environment

1. Go to your GitHub repository
2. Navigate to **Settings** → **Environments**
3. Click **New environment**
4. Name: `production`
5. (Optional) Add protection rules:
   - Required reviewers
   - Wait timer: 5 minutes
   - Deployment branches: `main` only

#### 2.2 Add Repository Secret

1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Add:
   - **Name:** `SNOWFLAKE_ACCOUNT`
   - **Value:** Your Snowflake account identifier (e.g., `abc12345.us-east-1` or `abc12345.snowflakecomputing.com`)

---

### Step 3: Test the Workflow

#### Option A: Manual Trigger
1. Go to **Actions** tab
2. Select **O2C Enhanced - dbt Deploy**
3. Click **Run workflow** → **Run workflow**
4. Watch the logs

#### Option B: Push to Main
```bash
# Make a small change
git add .
git commit -m "ci: test GitHub Actions workflow"
git push origin main
```

---

## 📊 Workflow Features

| Feature | Description |
|---------|-------------|
| **OIDC Authentication** | Secure, no secrets needed |
| **Multi-Job Pipeline** | Core → Semantic Views (in order) |
| **PR Validation** | Syntax check on pull requests |
| **Scheduled Runs** | Daily at 6 AM UTC |
| **Manual Triggers** | On-demand with full-refresh option |
| **Artifact Upload** | dbt manifest, run results, catalog |
| **Job Summaries** | Rich markdown summaries in GitHub |

---

## 🔧 Customization

### Change Schedule
Edit `.github/workflows/dbt_o2c_deploy.yml`:
```yaml
schedule:
  - cron: '0 8 * * *'  # Change to 8 AM UTC
```

### Add Slack Notifications
Add to workflow:
```yaml
- name: Notify Slack on Failure
  if: failure()
  uses: 8398a7/action-slack@v3
  with:
    status: failure
    fields: repo,message,commit,author
  env:
    SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK }}
```

### Run Only Changed Models (PR)
```yaml
- name: dbt Build Modified Only
  run: dbt build --select state:modified+
```

---

## 🔍 Troubleshooting

### "OIDC token is invalid"
- Verify subject matches exactly: `repo:<owner>/<repo>:environment:<environment>`
- Check GitHub environment name matches workflow

### "User does not exist"
- Ensure Snowflake user was created with correct name
- Check user TYPE = SERVICE

### "Insufficient privileges"
- Verify GITHUB_CICD_ROLE has required grants
- Check role is granted to user

### Generate Subject String
```bash
gh repo view --json nameWithOwner | jq -r '"repo:\(.nameWithOwner):environment:production"'
```

---

## 📁 File Structure

```
.github/
├── workflows/
│   └── dbt_o2c_deploy.yml    # Main CI/CD workflow
└── CICD_SETUP.md             # This guide
```

---

## ✅ Setup Checklist

### Snowflake
- [ ] Created GITHUB_CICD_ROLE
- [ ] Granted schema permissions
- [ ] Created github_actions_service_user with OIDC
- [ ] Granted role to user

### GitHub
- [ ] Created 'production' environment
- [ ] Added SNOWFLAKE_ACCOUNT secret
- [ ] Workflow file exists in .github/workflows/

### Testing
- [ ] Manual trigger works
- [ ] Push to main triggers workflow
- [ ] Models deploy successfully
- [ ] Tests pass

---

## 🎉 You're Done!

Once setup is complete:
- ✅ Push to `main` → Automatic deployment
- ✅ Open PR → Syntax validation
- ✅ Daily at 6 AM → Scheduled refresh
- ✅ Manual trigger → On-demand builds

**No more manual `dbt build` commands needed!** 🚀

