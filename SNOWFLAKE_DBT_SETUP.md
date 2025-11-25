# Running DBT Projects in Snowflake (Native Integration)

## Overview

This guide shows how to run the DBT projects using **Snowflake's native DBT integration** instead of local DBT installation. This approach:
- ✅ No local Python/DBT installation needed
- ✅ Run DBT directly in Snowflake UI
- ✅ Built-in version control
- ✅ Integrated with Snowflake compute

---

## Prerequisites

1. Snowflake account with **ACCOUNTADMIN** or appropriate role
2. Git repository (GitHub, GitLab, or Bitbucket) - optional but recommended
3. Web browser

---

## Step 1: Prepare Your Data (Run data_prep.sql)

### 1.1 Open Snowflake Web UI
```
https://<your_account>.snowflakecomputing.com
```

### 1.2 Create a New Worksheet
- Click **Worksheets** in left navigation
- Click **+ Worksheet** button

### 1.3 Copy and Run data_prep.sql
```sql
-- Copy entire contents of implementation/data_prep.sql
-- Paste into Snowflake worksheet
-- Click "Run All" (or Ctrl+Enter)
```

### 1.4 Verify Data Loaded
```sql
-- Should return 4 tables with data
SELECT 'FACT_AR' AS table_name, COUNT(*) FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
UNION ALL
SELECT 'DIM_CUSTOMER', COUNT(*) FROM EDW.CORP_MASTER.DIM_CUSTOMER
UNION ALL
SELECT 'DIM_ENTITY', COUNT(*) FROM EDW.CORP_MASTER.DIM_ENTITY
UNION ALL
SELECT 'TIME_FISCAL_DAY', COUNT(*) FROM EDW.CORP_REF.TIME_FISCAL_DAY;

-- Expected:
-- FACT_AR          | 250
-- DIM_CUSTOMER     | 7
-- DIM_ENTITY       | 6
-- TIME_FISCAL_DAY  | 730
```

---

## Step 2: Set Up Git Repository (Recommended)

### 2.1 Create GitHub Repository
```bash
# On your local machine or GitHub web UI:
# 1. Create new repository: "honeywell-dbt-foundation"
# 2. Create another repository: "honeywell-dbt-finance-core"
```

### 2.2 Push DBT Projects to Git
```bash
# For Foundation project
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/dbt_foundation
git init
git add .
git commit -m "Initial foundation project"
git remote add origin https://github.com/<your-org>/honeywell-dbt-foundation.git
git push -u origin main

# For Finance Core project
cd ../dbt_finance_core
git init
git add .
git commit -m "Initial finance core project"
git remote add origin https://github.com/<your-org>/honeywell-dbt-finance-core.git
git push -u origin main
```

---

## Step 3: Create DBT Foundation Project in Snowflake

### 3.1 Navigate to Projects
1. In Snowflake UI, click **Projects** in left navigation
2. Click **+ Project** button (top right)
3. Select **DBT Project**

### 3.2 Configure Foundation Project

**Project Settings:**
```
Project Name: dbt_foundation
Description: Foundation project with shared staging and dimensions
```

**Connection Settings:**
```
Database: EDW
Schema: DBT_STAGING (for development)
Warehouse: COMPUTE_WH
Role: SYSADMIN (or DBT_ROLE if you created it)
```

**Git Integration (if using Git):**
```
Repository Type: GitHub (or your Git provider)
Repository URL: https://github.com/<your-org>/honeywell-dbt-foundation
Branch: main
Authentication: Personal Access Token (or SSH)
```

**OR Manual Upload (if not using Git):**
- Click **Upload Files**
- Upload all files from `implementation/dbt_foundation/` directory
- Maintain the folder structure

### 3.3 Initialize Project
1. Click **Create Project**
2. Wait for Snowflake to clone/initialize the project
3. You should see the project file explorer

---

## Step 4: Configure Foundation Project Files

### 4.1 Update dbt_project.yml (in Snowflake UI)

The file should already be correct, but verify:
```yaml
name: 'dbt_foundation'
version: '1.0.0'
config-version: 2

profile: 'snowflake_connection'  # Snowflake will auto-configure this

models:
  dbt_foundation:
    staging:
      +materialized: view
      +schema: dbt_staging
    marts:
      shared:
        +materialized: table
        +schema: dbt_shared
        +access: public
```

### 4.2 Configure Development Environment
1. Click **Development** tab in your project
2. Snowflake will auto-create a development schema
3. Select the warehouse: **COMPUTE_WH**

---

## Step 5: Install Dependencies & Run Foundation Project

### 5.1 Install Packages
1. In the Snowflake DBT project UI, click **Terminal** or **Console**
2. Run:
```bash
dbt deps
```

Expected output:
```
Installing dbt-labs/dbt_utils
Installing calogica/dbt_expectations
Installed 2 packages
```

### 5.2 Test Connection
```bash
dbt debug
```

Expected output:
```
All checks passed!
```

### 5.3 Run Foundation Models
```bash
# Run all foundation models
dbt run

# Or run specific models
dbt run --select staging.stg_ar.*
dbt run --select marts.shared.*
```

Expected output:
```
Running with dbt=1.7.0
Found 3 models, 12 tests, 2 macros

15:23:01  1 of 3 START sql view model dbt_staging.stg_ar_invoice ........ [RUN]
15:23:02  1 of 3 OK created sql view model dbt_staging.stg_ar_invoice ... [SUCCESS 1.2s]
15:23:02  2 of 3 START sql table model dbt_shared.dim_customer .......... [RUN]
15:23:04  2 of 3 OK created sql table model dbt_shared.dim_customer ..... [SUCCESS 2.1s]
15:23:04  3 of 3 START sql table model dbt_shared.dim_fiscal_calendar ... [RUN]
15:23:06  3 of 3 OK created sql table model dbt_shared.dim_fiscal_calendar [SUCCESS 1.8s]

Completed successfully
```

### 5.4 Run Tests
```bash
dbt test
```

Expected output:
```
Running with dbt=1.7.0
Found 12 tests

15:24:01  1 of 12 START test not_null_stg_ar_invoice_source_system ..... [RUN]
15:24:02  1 of 12 PASS not_null_stg_ar_invoice_source_system ........... [PASS]
...
15:24:10  12 of 12 PASS unique_combination_dim_customer ................ [PASS]

Completed successfully
```

### 5.5 Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

This will create an interactive documentation site with lineage graphs.

---

## Step 6: Create DBT Finance Core Project in Snowflake

### 6.1 Create New DBT Project
1. Click **Projects** in left navigation
2. Click **+ Project** button
3. Select **DBT Project**

### 6.2 Configure Finance Core Project

**Project Settings:**
```
Project Name: dbt_finance_core
Description: Finance domain data marts
```

**Connection Settings:**
```
Database: EDW
Schema: CORP_DM_FIN
Warehouse: COMPUTE_WH
Role: SYSADMIN (or DBT_ROLE)
```

**Git Integration (if using Git):**
```
Repository URL: https://github.com/<your-org>/honeywell-dbt-finance-core
Branch: main
```

**OR Upload Files:**
- Upload all files from `implementation/dbt_finance_core/`

### 6.3 Configure Cross-Project Reference

In Snowflake DBT, you need to configure the dependency on foundation project.

**Edit dependencies.yml:**
```yaml
projects:
  # For Snowflake native DBT, reference by project name
  - name: dbt_foundation
```

**Important**: Snowflake will automatically resolve cross-project references.

---

## Step 7: Run Finance Core Project

### 7.1 Install Dependencies
```bash
cd dbt_finance_core  # In Snowflake UI
dbt deps
```

This will resolve the dependency on `dbt_foundation`.

### 7.2 Run Finance Models
```bash
dbt run
```

Expected output:
```
Running with dbt=1.7.0
Found 1 model, 8 tests

15:25:01  1 of 1 START sql table model dm_fin.dm_fin_ar_aging_simple ... [RUN]
15:25:04  1 of 1 OK created sql table model dm_fin.dm_fin_ar_aging_simple [SUCCESS 3.2s]

Completed successfully
```

### 7.3 Run Tests
```bash
dbt test
```

Expected: All 8 tests pass ✅

### 7.4 Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

---

## Step 8: Verify Results

### 8.1 Query the Data Mart in Snowflake
```sql
-- Use Snowflake worksheet
SELECT 
    source_system,
    aging_bucket,
    customer_type_flag,
    COUNT(*) AS invoice_count,
    ROUND(SUM(amt_usd_me), 2) AS total_amount,
    ROUND(AVG(days_late), 1) AS avg_days_late
FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

### 8.2 Check Lineage
1. In the Finance Core project
2. Click **Lineage** tab
3. You should see the full DAG from source to mart:
   ```
   FACT_AR → stg_ar_invoice → dm_fin_ar_aging_simple
   DIM_CUSTOMER → dim_customer → dm_fin_ar_aging_simple
   TIME_FISCAL_DAY → dim_fiscal_calendar → dm_fin_ar_aging_simple
   ```

---

## Alternative: Using Snowflake Worksheets (Without DBT Projects Feature)

If your Snowflake edition doesn't have the DBT Projects feature, you can still run DBT in Snowflake using **Snowflake CLI** or **Snowpark**:

### Option A: Compile DBT Locally, Run SQL in Snowflake

```bash
# On your local machine
cd implementation/dbt_foundation
dbt compile

# This generates compiled SQL in target/compiled/
# Copy those SQL files and run them manually in Snowflake worksheets
```

### Option B: Use Snowflake Notebooks (Python)

Create a Snowflake Notebook and run:

```python
import subprocess
import os

# Install dbt-snowflake
subprocess.run(["pip", "install", "dbt-snowflake"])

# Set up connection
os.environ['DBT_PROFILES_DIR'] = '/path/to/profiles'

# Run dbt
subprocess.run(["dbt", "run", "--project-dir", "/path/to/dbt_foundation"])
subprocess.run(["dbt", "run", "--project-dir", "/path/to/dbt_finance_core"])
```

---

## Snowflake DBT Project Structure (UI Navigation)

```
Snowflake UI
├── Projects
│   ├── dbt_foundation
│   │   ├── Files (file explorer)
│   │   │   ├── dbt_project.yml
│   │   │   ├── models/
│   │   │   └── macros/
│   │   ├── Development (run dbt commands)
│   │   ├── Lineage (visual DAG)
│   │   ├── Documentation (dbt docs)
│   │   └── Settings
│   │
│   └── dbt_finance_core
│       ├── Files
│       ├── Development
│       ├── Lineage
│       ├── Documentation
│       └── Settings
│
├── Worksheets (for SQL queries)
└── Data (view tables/schemas)
```

---

## Troubleshooting in Snowflake DBT

### Issue: "Project not found" in dependencies
```yaml
# In dependencies.yml, ensure project name matches exactly
projects:
  - name: dbt_foundation  # Must match the Snowflake project name
```

### Issue: "Schema does not exist"
```sql
-- Create missing schemas
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS EDW.DBT_STAGING;
CREATE SCHEMA IF NOT EXISTS EDW.DBT_SHARED;
CREATE SCHEMA IF NOT EXISTS EDW.CORP_DM_FIN;
```

### Issue: "Permission denied"
```sql
-- Grant permissions
GRANT USAGE ON DATABASE EDW TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA EDW.DBT_STAGING TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA EDW.DBT_SHARED TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA EDW.CORP_DM_FIN TO ROLE SYSADMIN;
```

### Issue: Cross-project reference not working
1. Ensure foundation project ran successfully first
2. Verify foundation models have `+access: public` in config
3. Run `dbt deps` in finance core project
4. Check that both projects use same database/warehouse

---

## Best Practices for Snowflake DBT

### 1. Use Separate Projects
- ✅ Keep foundation and domain projects separate
- ✅ Each project in its own Git repository
- ✅ Clear boundaries and dependencies

### 2. Use Development Schemas
- ✅ Snowflake auto-creates dev schemas per user
- ✅ Test changes before merging to main
- ✅ Use `--target dev` vs `--target prod`

### 3. Leverage Snowflake Compute
- ✅ Use appropriate warehouse sizes
- ✅ XS for development
- ✅ S-M for production
- ✅ Auto-suspend after 1 minute

### 4. Version Control
- ✅ Commit all changes to Git
- ✅ Use pull requests for code review
- ✅ Tag releases (v1.0.0, v1.1.0, etc.)

---

## Deployment Workflow in Snowflake

### Development → Staging → Production

```bash
# Development (your dev schema)
dbt run --target dev

# Staging (shared staging schema)
dbt run --target staging

# Production (production schemas)
dbt run --target prod
```

**Configure targets in dbt_project.yml:**
```yaml
target-path: "target"

# Use Snowflake environment variables
vars:
  env: "{{ env_var('DBT_ENV', 'dev') }}"
```

---

## Summary: Snowflake DBT vs Local DBT

| Feature | Local DBT | Snowflake DBT |
|---------|-----------|---------------|
| Installation | pip install dbt-snowflake | Built-in, no install |
| IDE | VS Code, local editor | Snowflake Web UI |
| Git Integration | Manual push/pull | Automatic sync |
| Compute | Local compilation | Snowflake compute |
| Collaboration | Share code manually | Built-in sharing |
| Documentation | Local server | Integrated in UI |
| Cost | Free (except Snowflake) | Included in Snowflake |

---

## Next Steps

1. ✅ Run `data_prep.sql` in Snowflake
2. ✅ Create `dbt_foundation` project in Snowflake UI
3. ✅ Run `dbt run` and `dbt test`
4. ✅ Create `dbt_finance_core` project in Snowflake UI
5. ✅ Run `dbt run` and `dbt test`
6. ✅ Query final data mart
7. ✅ View lineage and documentation

---

**Ready to start!** Open Snowflake and begin with Step 1. 🚀

