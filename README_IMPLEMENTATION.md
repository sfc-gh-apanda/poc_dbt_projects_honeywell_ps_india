# Implementation Guide: Smallest Isolated Branch (AR Aging)

## Overview

This implementation creates the **smallest complete branch** from root to leaf:

```
Source (FACT_AR) 
    ↓
dbt_foundation (staging + shared dimensions)
    ↓
dbt_finance_core (AR Aging data mart)
```

## What's Included

### 1. Foundation Project
- **Staging**: `stg_ar_invoice` (AR invoice base)
- **Shared Dimensions**: `dim_customer`, `dim_fiscal_calendar`
- **Macros**: `aging_bucket()`, `fiscal_period()`

### 2. Finance Core Project  
- **Data Mart**: `dm_fin_ar_aging_simple` (simplified AR aging report)

### 3. Data Prep
- SQL script to create and populate all source tables

## Directory Structure

```
implementation/
├── data_prep.sql                           # Run this first in Snowflake
├── README_IMPLEMENTATION.md                # This file
│
├── dbt_foundation/                         # Foundation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml               # Source definitions
│   │   │   └── stg_ar/
│   │   │       └── stg_ar_invoice.sql     # AR staging
│   │   │
│   │   └── marts/
│   │       └── shared/
│   │           ├── _shared.yml            # Published models
│   │           ├── dim_customer.sql       # Customer dimension
│   │           └── dim_fiscal_calendar.sql # Fiscal calendar
│   │
│   └── macros/
│       ├── aging_bucket.sql               # Aging bucket logic
│       └── fiscal_period.sql              # Fiscal period lookup
│
└── dbt_finance_core/                       # Finance domain project
    ├── dbt_project.yml
    ├── dependencies.yml                    # Depends on foundation
    │
    └── models/
        └── marts/
            └── finance/
                ├── _finance.yml            # Model docs & tests
                └── dm_fin_ar_aging_simple.sql  # AR aging mart
```

## Installation & Setup

### Prerequisites
1. Snowflake account with appropriate permissions
2. Python 3.8+ installed
3. Git installed

### Step 1: Prepare Data (5 minutes)

```bash
# Connect to Snowflake and run data prep script
snowsql -a <your_account> -u <your_user>

# In Snowflake, run:
!source implementation/data_prep.sql

# This creates:
# - 4 source tables with sample data
# - 250 AR invoices across 3 systems
# - 7 customers, 6 entities
# - 730 days of fiscal calendar
```

### Step 2: Set Up Foundation Project (10 minutes)

```bash
# Navigate to foundation directory
cd implementation/dbt_foundation

# Install dbt-snowflake
pip install dbt-snowflake

# Configure profiles
# Edit ~/.dbt/profiles.yml with your Snowflake credentials
# (See profiles.yml template in dbt_foundation/)

# Install packages
dbt deps

# Test connection
dbt debug

# Run foundation models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

**Expected Output:**
```
Running with dbt=1.7.0
Found 3 models, 12 tests, 2 macros
Completed successfully

03:15:23  1 of 3 START sql view model dbt_staging.stg_ar_invoice ........ [RUN]
03:15:24  1 of 3 OK created sql view model dbt_staging.stg_ar_invoice ... [SUCCESS in 1.2s]
03:15:24  2 of 3 START sql table model dbt_shared.dim_customer .......... [RUN]
03:15:26  2 of 3 OK created sql table model dbt_shared.dim_customer ..... [SUCCESS in 2.1s]
03:15:26  3 of 3 START sql table model dbt_shared.dim_fiscal_calendar ... [RUN]
03:15:28  3 of 3 OK created sql table model dbt_shared.dim_fiscal_calendar [SUCCESS in 1.8s]

Finished running 1 view model, 2 table models in 5.2s.
Completed successfully
```

### Step 3: Set Up Finance Core Project (10 minutes)

```bash
# Navigate to finance core directory
cd ../dbt_finance_core

# Install foundation dependency
dbt deps

# Test connection
dbt debug

# Run finance models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

**Expected Output:**
```
Running with dbt=1.7.0
Found 1 model, 8 tests, 0 macros (+ 2 from dependencies)
Completed successfully

03:16:15  1 of 1 START sql table model dm_fin.dm_fin_ar_aging_simple ... [RUN]
03:16:18  1 of 1 OK created sql table model dm_fin.dm_fin_ar_aging_simple [SUCCESS in 3.2s]

Finished running 1 table model in 3.2s.
Completed successfully
```

### Step 4: Verify Results (5 minutes)

```sql
-- In Snowflake, query the final data mart
SELECT 
    source_system,
    aging_bucket,
    customer_type_flag,
    COUNT(*) AS invoice_count,
    SUM(amt_usd_me) AS total_amount
FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Expected output:
-- BRP900 | CURRENT | EXTERNAL | 45 | $2,145,000
-- BRP900 | 1-30    | EXTERNAL | 12 | $567,890
-- ... etc
```

## Project Configuration Details

### Foundation Project Configuration

```yaml
# dbt_foundation/dbt_project.yml
name: dbt_foundation
version: 1.0.0

models:
  dbt_foundation:
    staging:
      +materialized: view
      +schema: dbt_staging
      +access: private
    
    marts:
      shared:
        +materialized: table
        +schema: dbt_shared
        +access: public      # Published for other projects
        +contract:
          enforced: true     # Schema contracts
```

### Finance Core Project Configuration

```yaml
# dbt_finance_core/dbt_project.yml
name: dbt_finance_core
version: 1.0.0

models:
  dbt_finance_core:
    marts:
      finance:
        +materialized: table
        +schema: dm_fin
        +cluster_by: ['source_system', 'snapshot_date']
```

### Dependencies

```yaml
# dbt_finance_core/dependencies.yml
projects:
  - name: dbt_foundation
    version: ">=1.0.0,<2.0.0"
```

## Testing Strategy

### Foundation Tests
- Source freshness checks
- Not null on primary keys
- Unique combination of keys
- Referential integrity

### Finance Core Tests
- Not null on critical fields
- Accepted values for aging buckets
- Amount validations (non-negative)
- Unique combination of keys
- Custom test: sum(amt) reconciles to source

## Common Issues & Troubleshooting

### Issue 1: "Database does not exist"
```bash
# Ensure data_prep.sql was run successfully
# Check database exists:
SHOW DATABASES LIKE 'EDW';
```

### Issue 2: "Permission denied"
```bash
# Ensure DBT_ROLE has proper grants
# Re-run Step 5 from data_prep.sql
```

### Issue 3: "Model not found in dependency"
```bash
# Ensure foundation project ran first
cd dbt_foundation && dbt run
cd dbt_finance_core && dbt deps && dbt run
```

### Issue 4: "No data in final table"
```bash
# Check source data exists
SELECT COUNT(*) FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL;
# Should return 250

# Check filters in staging model
# Ensure CLEARING_DATE IS NULL returns records
```

## Performance Benchmarks

Expected run times on XS warehouse:

| Project | Models | Compile | Run | Test | Total |
|---------|--------|---------|-----|------|-------|
| Foundation | 3 | 5s | 5s | 3s | 13s |
| Finance Core | 1 | 3s | 3s | 2s | 8s |
| **Total** | **4** | **8s** | **8s** | **5s** | **21s** |

## Data Validation

Run these queries to validate the implementation:

```sql
-- 1. Check staging layer
SELECT COUNT(*) FROM EDW.DBT_STAGING.STG_AR_INVOICE;
-- Expected: 250

-- 2. Check shared dimensions
SELECT COUNT(*) FROM EDW.DBT_SHARED.DIM_CUSTOMER;
-- Expected: 7

SELECT COUNT(*) FROM EDW.DBT_SHARED.DIM_FISCAL_CALENDAR;
-- Expected: 730

-- 3. Check data mart
SELECT COUNT(*) FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE;
-- Expected: 250

-- 4. Validate aging calculation
SELECT 
    aging_bucket,
    COUNT(*) AS count
FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE
GROUP BY 1
ORDER BY 
    CASE aging_bucket
        WHEN 'CURRENT' THEN 1
        WHEN '1-30' THEN 2
        WHEN '31-60' THEN 3
        WHEN '61-90' THEN 4
        WHEN '91-120' THEN 5
        WHEN '121-150' THEN 6
        WHEN '151-180' THEN 7
        WHEN '181-360' THEN 8
        WHEN '361+' THEN 9
    END;

-- 5. Reconcile amounts
SELECT 
    'Source' AS layer,
    SUM(AMT_USD_ME) AS total_amt
FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
UNION ALL
SELECT 
    'Staging' AS layer,
    SUM(amt_usd_me) AS total_amt
FROM EDW.DBT_STAGING.STG_AR_INVOICE
UNION ALL
SELECT 
    'Data Mart' AS layer,
    SUM(amt_usd_me) AS total_amt
FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE;
-- All three should match!
```

## Next Steps

After successfully implementing this branch, you can:

1. **Add more fields** to the simplified AR aging model
2. **Add dispute data** (create stg_ar_dispute)
3. **Add more domains** (GL, Revenue, etc.)
4. **Implement incremental materialization** for large volumes
5. **Add snapshots** for historical tracking
6. **Set up CI/CD** for automated deployment

## Success Criteria

✅ Foundation project builds successfully  
✅ Finance core project builds successfully  
✅ All tests pass  
✅ Data reconciles from source to mart  
✅ Documentation generated and accessible  
✅ Total build time < 30 seconds  
✅ Zero cross-domain dependencies (only foundation)  

---

**Congratulations!** You've successfully implemented the smallest isolated branch. This serves as the template for all future domain implementations.

