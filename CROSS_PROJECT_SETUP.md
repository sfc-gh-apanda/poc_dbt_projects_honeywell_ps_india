# Cross-Project Reference Setup Guide

## Problem: `dbt_finance_core` can't find `stg_ar_invoice` from `dbt_foundation`

**Error:**
```
Model 'model.dbt_finance_core.dm_fin_ar_aging_simple' depends on a node named 
'stg_ar_invoice' in package or project 'dbt_foundation' which was not found.
```

---

## Root Cause

Cross-project references (`ref('dbt_foundation', 'model_name')`) require:
1. ✅ The referenced project must be **built first**
2. ✅ The referenced model must have `access: public`
3. ✅ The dependency must be declared in `dependencies.yml`
4. ✅ Both projects must share the same dbt version
5. ✅ The manifest.json from the dependency must be available

---

## Solution Path 1: Local Development (Recommended)

### Step 1: Ensure Both Projects Are in Same Parent Directory

Your current structure (already correct):
```
/Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/
├── dbt_foundation/
│   ├── dbt_project.yml (name: dbt_foundation)
│   └── models/staging/stg_ar/stg_ar_invoice.sql
└── dbt_finance_core/
    ├── dbt_project.yml (name: dbt_finance_core)
    └── dependencies.yml (references dbt_foundation)
```

### Step 2: Install Dependencies

```bash
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/dbt_finance_core

# This tells dbt to look for dbt_foundation locally
dbt deps
```

### Step 3: Set DBT_PROJECT_DIR Environment Variable

```bash
# Export the parent directory containing both projects
export DBT_PROJECT_DIR=/Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation
```

### Step 4: Build Foundation First

```bash
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/dbt_foundation

# Build foundation (creates manifest.json)
dbt build --target dev

# Verify models were created
dbt ls --select stg_ar_invoice
```

### Step 5: Build Finance Core

```bash
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/dbt_finance_core

# Now finance_core can find foundation models
dbt build --target dev
```

---

## Solution Path 2: Snowflake Native DBT

### Prerequisites

1. Both Git repositories must be set up in Snowflake
2. Both DBT projects must be created in Snowflake
3. Foundation project must be built before finance core

### Step 1: Refresh Git Repositories in Snowflake

```sql
USE ROLE git_admin;
USE DATABASE DEPLOYMENT_DB;
USE SCHEMA GIT_SCHEMA;

-- Fetch latest changes (includes all recent fixes)
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;
ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;

-- Verify files updated
LS @dbt_foundation_repo/branches/main/dbt_foundation/models/staging/stg_ar/;
-- Should show: stg_ar_invoice.sql, _stg_ar.yml

-- Check the access level was changed
SELECT $1 FROM @dbt_foundation_repo/branches/main/dbt_foundation/dbt_project.yml
WHERE $1 LIKE '%+access: public%';
```

### Step 2: Create/Update Snowflake DBT Projects

**Create Foundation Project:**
```sql
USE ROLE accountadmin;

-- Create or replace foundation project
CREATE OR REPLACE DBT PROJECT dbt_foundation_project
    USING GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_foundation_repo
    BRANCH = 'main'
    DATABASE = EDW
    WAREHOUSE = COMPUTE_WH
    ROOT_PATH = 'dbt_foundation'  -- Important: subdirectory path!
    SCHEMA = DBT_STAGING;

SHOW DBT PROJECTS LIKE 'dbt_foundation_project';
```

**Create Finance Core Project:**
```sql
-- Create or replace finance core project
CREATE OR REPLACE DBT PROJECT dbt_finance_core_project
    USING GIT REPOSITORY DEPLOYMENT_DB.GIT_SCHEMA.dbt_finance_core_repo
    BRANCH = 'main'
    DATABASE = EDW
    WAREHOUSE = COMPUTE_WH
    ROOT_PATH = 'dbt_finance_core'  -- Important: subdirectory path!
    SCHEMA = CORP_DM_FIN;

SHOW DBT PROJECTS LIKE 'dbt_finance_core_project';
```

### Step 3: Link Projects (Cross-Project Dependency)

```sql
-- Link finance_core to foundation
ALTER DBT PROJECT dbt_finance_core_project
    ADD DEPENDENCY dbt_foundation_project;

-- Verify dependency
DESCRIBE DBT PROJECT dbt_finance_core_project;
```

### Step 4: Build Projects in Correct Order

```sql
-- Build foundation FIRST
EXECUTE DBT PROJECT dbt_foundation_project
    COMMAND = 'build'
    WAREHOUSE = COMPUTE_WH;

-- Check results
SHOW DBT EXECUTIONS FOR PROJECT dbt_foundation_project;

-- Verify staging model was created
SELECT * FROM EDW.DBT_STAGING.STG_AR_INVOICE LIMIT 5;

-- Build finance core SECOND (can now find foundation models)
EXECUTE DBT PROJECT dbt_finance_core_project
    COMMAND = 'build'
    WAREHOUSE = COMPUTE_WH;

-- Check results
SHOW DBT EXECUTIONS FOR PROJECT dbt_finance_core_project;
```

---

## Solution Path 3: Using profiles.yml Approach

If Snowflake native DBT doesn't support cross-project refs, use local dbt with Snowflake connection:

### Step 1: Set Up Profiles

```bash
# Copy sample profile to ~/.dbt/
cp dbt_foundation/profiles.yml ~/.dbt/profiles_foundation.yml
cp dbt_finance_core/profiles.yml ~/.dbt/profiles_finance.yml

# Or merge into single profiles.yml with both profiles
cat > ~/.dbt/profiles.yml << 'EOF'
dbt_foundation:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USER
      password: YOUR_PASSWORD
      role: DBT_DEV_ROLE
      database: EDW
      warehouse: COMPUTE_WH
      schema: DBT_STAGING
      threads: 4

dbt_finance_core:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USER
      password: YOUR_PASSWORD
      role: DBT_DEV_ROLE
      database: EDW
      warehouse: COMPUTE_WH
      schema: CORP_DM_FIN
      threads: 4
EOF
```

### Step 2: Build in Order

```bash
# Build foundation
cd dbt_foundation
dbt deps
dbt build --profiles-dir ~/.dbt

# Build finance core
cd ../dbt_finance_core
dbt deps
dbt build --profiles-dir ~/.dbt
```

---

## Verification Checklist

### ✅ Pre-Build Checks

```bash
# 1. Check dbt version is same in both projects
cd dbt_foundation && dbt --version
cd ../dbt_finance_core && dbt --version

# 2. Verify dependencies.yml exists
cat dbt_finance_core/dependencies.yml

# 3. Check access level in foundation
grep -A 2 "staging:" dbt_foundation/dbt_project.yml
# Should show: +access: public

# 4. Verify model documentation exists
ls dbt_foundation/models/staging/stg_ar/_stg_ar.yml

# 5. Check cross-project ref syntax
grep "ref('dbt_foundation'" dbt_finance_core/models/marts/finance/dm_fin_ar_aging_simple.sql
```

### ✅ Post-Build Checks

```bash
# 1. Foundation manifest exists
ls dbt_foundation/target/manifest.json

# 2. Staging model in manifest
cat dbt_foundation/target/manifest.json | grep stg_ar_invoice

# 3. Finance core can compile
cd dbt_finance_core
dbt compile --select dm_fin_ar_aging_simple

# 4. Check compiled SQL
cat target/compiled/dbt_finance_core/models/marts/finance/dm_fin_ar_aging_simple.sql
```

---

## Common Issues and Fixes

### Issue 1: "Project dbt_foundation not found"

**Cause:** Foundation hasn't been built yet

**Fix:**
```bash
cd dbt_foundation
dbt build
cd ../dbt_finance_core
dbt build
```

### Issue 2: "Access level not public"

**Cause:** Model is marked as private

**Fix:** Already applied in commit `c0e01d1`
```yaml
# dbt_foundation/dbt_project.yml
staging:
  +access: public  # ✅ Changed from private
```

### Issue 3: "Model not found in manifest"

**Cause:** Model needs YAML documentation for cross-project discovery

**Fix:** Already applied in commit `c0e01d1`
```yaml
# dbt_foundation/models/staging/stg_ar/_stg_ar.yml
models:
  - name: stg_ar_invoice
    access: public  # ✅ Added
```

### Issue 4: "Cannot find manifest.json"

**Cause:** Foundation project not built in accessible location

**Fix:**
```bash
# Option A: Build foundation first
cd dbt_foundation && dbt build

# Option B: Use --state flag
cd dbt_finance_core
dbt build --state ../dbt_foundation/target
```

### Issue 5: Snowflake Native DBT - Dependency Not Linked

**Cause:** Projects not linked in Snowflake

**Fix:**
```sql
ALTER DBT PROJECT dbt_finance_core_project
    ADD DEPENDENCY dbt_foundation_project;
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────┐
│         dbt_foundation                  │
│  ┌───────────────────────────────────┐  │
│  │ staging/stg_ar_invoice            │  │
│  │ access: public ✅                 │  │
│  └───────────────────────────────────┘  │
│  ┌───────────────────────────────────┐  │
│  │ marts/dim_customer                │  │
│  │ access: public ✅                 │  │
│  └───────────────────────────────────┘  │
│  ┌───────────────────────────────────┐  │
│  │ marts/dim_fiscal_calendar         │  │
│  │ access: public ✅                 │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
                    ↓
        ref('dbt_foundation', 'model')
                    ↓
┌─────────────────────────────────────────┐
│         dbt_finance_core                │
│  ┌───────────────────────────────────┐  │
│  │ dm_fin_ar_aging_simple            │  │
│  │ depends on: ↑ all above models    │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

---

## Latest Commits Applied

| Commit | Fix |
|--------|-----|
| `c0e01d1` | Changed staging access to public + added _stg_ar.yml |
| `049a609` | Fixed SQL self-referencing alias error |
| `c5ed30a` | Fixed timestamp_ntz casting |
| `d410f1e` | Added missing project structure files |

---

## Quick Command Reference

```bash
# Local: Build both projects
cd dbt_foundation && dbt build && cd ../dbt_finance_core && dbt build

# Snowflake: Refresh Git
ALTER GIT REPOSITORY dbt_foundation_repo FETCH;
ALTER GIT REPOSITORY dbt_finance_core_repo FETCH;

# Snowflake: Build projects
EXECUTE DBT PROJECT dbt_foundation_project COMMAND = 'build';
EXECUTE DBT PROJECT dbt_finance_core_project COMMAND = 'build';
```

---

## Success Criteria

✅ `dbt_foundation` builds successfully  
✅ `stg_ar_invoice` is created in `EDW.DBT_STAGING`  
✅ `dbt_finance_core` finds the dependency  
✅ `dm_fin_ar_aging_simple` compiles and runs  
✅ All tests pass  

---

**Need Help?**

Check detailed logs:
```bash
cat logs/dbt.log | grep -i error
cat logs/dbt.log | grep -i "stg_ar_invoice"
```

