# GitHub Actions OIDC Setup - Quick Start Checklist

**Estimated Time:** 10 minutes  
**Authentication:** OIDC (Secure, No Private Keys)

---

## ✅ Pre-Flight Check

Before starting, ensure you have:
- [ ] Snowflake ACCOUNTADMIN access
- [ ] GitHub repository admin access
- [ ] `snowsql` CLI installed (or Snowsight access)
- [ ] Your Snowflake account identifier ready

---

## 📋 Setup Steps (Do in Order)

### 1️⃣ Snowflake Configuration (3 minutes)

```bash
# Option A: Run via snowsql
cd .github
snowsql -f GITHUB_ACTIONS_SETUP.sql

# Option B: Copy-paste into Snowsight
# Open GITHUB_ACTIONS_SETUP.sql and run all commands
```

**Expected output:** 
```
✅ Step 1 Complete: GITHUB_CICD_ROLE created
✅ Step 3 Complete: Service user created with OIDC
✅ GITHUB ACTIONS OIDC SETUP COMPLETE!
```

---

### 2️⃣ Get Your Snowflake Account Identifier (30 seconds)

```sql
-- Run in Snowflake
SELECT CURRENT_ACCOUNT();
```

**Example output:** `ABC12345` or `ORGNAME-ACCOUNTNAME`

**Save this!** You'll need it for GitHub secrets.

---

### 3️⃣ Create GitHub Environment (2 minutes)

1. Go to: `https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india/settings/environments`
2. Click **"New environment"**
3. Name: `production`
4. Click **"Configure environment"**
5. (Optional) Add protection rules:
   - ☑️ Required reviewers (add yourself)
   - ☑️ Deployment branches: Select "Selected branches" → Add `main`
6. Click **"Save protection rules"**

---

### 4️⃣ Add GitHub Secret (1 minute)

1. Go to: `https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india/settings/secrets/actions`
2. Click **"New repository secret"**
3. Fill in:
   - **Name:** `SNOWFLAKE_ACCOUNT`
   - **Value:** (paste your account identifier from Step 2)
4. Click **"Add secret"**

**Verify:** You should see `SNOWFLAKE_ACCOUNT` listed under "Repository secrets"

---

### 5️⃣ Verify Workflow File Exists (30 seconds)

Check that this file exists in your repo:
```
.github/workflows/dbt_o2c_deploy.yml ✅
```

**If missing:** The file should have been created. Check your repository.

---

### 6️⃣ Test Manual Deployment (3 minutes)

1. Go to: `https://github.com/sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india/actions`
2. Click on **"O2C Enhanced - dbt Deploy (OIDC)"** workflow
3. Click **"Run workflow"** button (top right)
4. Select branch: `main`
5. Click green **"Run workflow"** button
6. Click on the running workflow to see live logs

**Watch for:**
- ✅ "Set up Snowflake CLI with OIDC" step completes
- ✅ "dbt Build" step completes
- ✅ "Deploy Semantic Views" job completes
- ✅ All jobs show green checkmarks

---

### 7️⃣ Verify in Snowflake (1 minute)

```sql
-- Check if tables were created/updated
USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

SELECT 
    TABLE_SCHEMA, 
    TABLE_NAME, 
    ROW_COUNT,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA LIKE 'O2C_ENHANCED%'
ORDER BY LAST_ALTERED DESC
LIMIT 20;

-- Expected: You should see O2C_ENHANCED tables with recent LAST_ALTERED timestamps
```

---

### 8️⃣ Test Automatic Trigger (2 minutes)

```bash
# Make a small change
echo "# CI/CD test - $(date)" >> O2C/dbt_o2c_enhanced/README.md

# Commit and push
git add .
git commit -m "test: trigger automatic CI/CD deployment"
git push origin main
```

**Then:**
1. Go to Actions tab immediately
2. Watch workflow start automatically
3. Verify it completes successfully

---

## 🎉 Success Criteria

You're done when ALL of these are true:

- ✅ Manual workflow trigger works
- ✅ Automatic trigger on push works
- ✅ No authentication errors in logs
- ✅ Tables updated in Snowflake with recent timestamps
- ✅ Workflow completes in < 10 minutes
- ✅ All jobs show green checkmarks

---

## 🚨 Quick Troubleshooting

### ❌ "Authentication failed"
**Fix:** Verify your OIDC subject matches exactly:
```sql
DESC USER github_actions_service_user;
-- Check WORKLOAD_IDENTITY field
-- Should be: repo:sfc-gh-apanda/poc_dbt_projects_honeywell_ps_india:environment:production
```

### ❌ "Permission denied"
**Fix:** Re-run grants in Snowflake:
```sql
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE EDW TO ROLE GITHUB_CICD_ROLE;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_CORE TO ROLE GITHUB_CICD_ROLE;
```

### ❌ Workflow doesn't trigger automatically
**Fix:** Check workflow file trigger section:
```yaml
on:
  push:
    branches:
      - main  # ← Must match your branch name
```

### ❌ "Environment not found"
**Fix:** 
1. GitHub environment must be named exactly `production`
2. Case-sensitive!
3. Re-create if needed

---

## 📞 Need Help?

1. **Check logs:** Actions tab → Click workflow run → Expand failed step
2. **Review setup:** Read `.github/README_CICD_SETUP.md` for details
3. **Verify config:** Compare your setup against this checklist

---

## 🔄 Daily Usage (After Setup)

Once set up, your workflow is:

```
1. Code → 2. Push → 3. Auto-deploy → 4. Done! ✅
```

**No manual steps needed!** Just push to main and watch it deploy.

---

## 📅 Scheduled Runs

Your workflow also runs automatically:
- **Schedule:** Daily at 6 AM UTC
- **Purpose:** Refresh data even without code changes
- **View schedule:** Check `.github/workflows/dbt_o2c_deploy.yml`

To change schedule:
```yaml
schedule:
  - cron: '0 6 * * *'  # ← Change this (cron format)
```

---

**Total setup time:** ~10 minutes  
**Maintenance:** Zero (fully automated)  
**Security:** ✅ OIDC (no secrets stored)

🎉 **You're all set! Happy deploying!**

