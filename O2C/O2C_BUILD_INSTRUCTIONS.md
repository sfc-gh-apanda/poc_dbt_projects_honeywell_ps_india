# 🚀 O2C Platform Build Instructions

**Date:** December 2, 2025  
**Project:** Honeywell O2C Analytics Platform

---

## ⚠️ IMPORTANT: Which Project to Build

### ✅ **BUILD FROM THIS:**

```bash
cd O2C/dbt_o2c
dbt deps
dbt build
```

### ❌ **DO NOT BUILD FROM THIS:**

```bash
cd O2C/dbt_o2c_semantic    # ❌ DEPRECATED - DO NOT USE!
```

**Why?** The `dbt_o2c_semantic` project is **deprecated**. All semantic views have been consolidated into `dbt_o2c` because **Snowflake Native dbt does not support cross-project references**.

---

## 📋 Step-by-Step Build Process

### **Step 1: Pull Latest Changes from Git**

In your Snowflake Worksheet:

```sql
-- Connect to your Git repository
USE DATABASE EDW;
USE SCHEMA PUBLIC;

-- Fetch latest changes
ALTER GIT REPOSITORY poc_dbt_projects FETCH;

-- Verify you have the latest
SHOW GIT BRANCHES IN poc_dbt_projects;
```

### **Step 2: Navigate to Correct Project**

**In your terminal/command line:**

```bash
# Start from the implementation folder
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation

# Navigate to the O2C dbt_o2c project
cd O2C/dbt_o2c

# Confirm you're in the right place
pwd
# Should show: .../implementation/O2C/dbt_o2c
```

### **Step 3: Install Dependencies**

```bash
dbt deps
```

**Expected output:**
```
Installing dbt-labs/dbt_utils@1.1.1
Installing calogica/dbt_expectations@0.10.1
Installing dbt-labs/codegen@0.12.1
Installing Snowflake-Labs/dbt_semantic_view@1.0.3    ← Important!

Updates available for packages: []

Up to date!
```

### **Step 4: Build All Models**

```bash
dbt build
```

**Expected output:**
```
Running with dbt=1.9.x

Found 10 models, 25 data tests, 3 sources, 0 exposures, 0 metrics, 2 semantic views

Concurrency: 4 threads

Building models:
  ✓ stg_enriched_orders ........................... [VIEW in 2.1s]
  ✓ stg_enriched_invoices ......................... [VIEW in 1.9s]
  ✓ stg_enriched_payments ......................... [VIEW in 2.0s]
  ✓ dim_o2c_customer .............................. [TABLE in 3.2s]
  ✓ dm_o2c_reconciliation ......................... [TABLE in 4.5s]
  ✓ dm_o2c_cycle_analysis ......................... [TABLE in 3.8s]
  ✓ agg_o2c_by_customer ........................... [TABLE in 2.9s]
  ✓ agg_o2c_by_period ............................. [TABLE in 2.7s]
  ✓ sv_o2c_reconciliation ......................... [SEMANTIC_VIEW in 5.1s]  ← New!
  ✓ sv_o2c_customer_summary ....................... [SEMANTIC_VIEW in 4.8s]  ← New!

Running tests:
  ✓ [25 tests passed]

✅ O2C Data Platform build complete!

Completed successfully

Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```

---

## ✅ Verification Queries

After successful build, run these in Snowsight:

### **1. Check All Schemas Were Created**

```sql
SELECT 
    schema_name,
    COUNT(*) as object_count
FROM EDW.INFORMATION_SCHEMA.TABLES
WHERE schema_name LIKE 'O2C_%'
GROUP BY schema_name
ORDER BY schema_name;
```

**Expected result:**
| SCHEMA_NAME | OBJECT_COUNT |
|-------------|--------------|
| O2C_AGGREGATES | 2 |
| O2C_CORE | 2 |
| O2C_DIMENSIONS | 1 |
| O2C_SEMANTIC_VIEWS | 2 |
| O2C_STAGING | 3 |

**Total: 5 schemas, 10 objects**

### **2. Verify Semantic Views Exist**

```sql
SHOW SEMANTIC VIEWS IN SCHEMA EDW.O2C_SEMANTIC_VIEWS;
```

**Expected result:**
- `SV_O2C_RECONCILIATION`
- `SV_O2C_CUSTOMER_SUMMARY`

### **3. Describe Semantic View Metadata**

```sql
DESCRIBE SEMANTIC VIEW EDW.O2C_SEMANTIC_VIEWS.SV_O2C_RECONCILIATION;
```

Should show:
- **DIMENSIONS**: customer_name, customer_type, customer_country, dates, etc.
- **FACTS**: order_amount, outstanding_amount, days_order_to_cash, etc.
- **METRICS**: total_revenue, avg_dso, total_ar_outstanding, etc.

### **4. Test Query on Semantic View**

```sql
SELECT 
    customer_name,
    customer_country,
    SUM(order_amount) as total_revenue,
    AVG(days_order_to_cash) as avg_dso,
    COUNT(DISTINCT order_key) as order_count
FROM EDW.O2C_SEMANTIC_VIEWS.SV_O2C_RECONCILIATION
WHERE customer_type = 'E'  -- External customers only
GROUP BY customer_name, customer_country
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## 📊 What Gets Built

| Layer | Schema | Models | Materialization |
|-------|--------|--------|-----------------|
| **Staging** | O2C_STAGING | 3 | VIEW |
| **Dimensions** | O2C_DIMENSIONS | 1 | TABLE |
| **Core Marts** | O2C_CORE | 2 | TABLE |
| **Aggregates** | O2C_AGGREGATES | 2 | TABLE |
| **Semantic Views** | O2C_SEMANTIC_VIEWS | 2 | SEMANTIC_VIEW |
| **TOTAL** | **5 schemas** | **10 models** | |

---

## 🤖 Cortex Analyst Integration

Once semantic views are deployed:

1. **Open Snowsight**
2. Click **+ Create** → **Cortex Analyst**
3. Select `EDW.O2C_SEMANTIC_VIEWS.SV_O2C_RECONCILIATION`
4. Start asking questions in natural language:

**Try these questions:**
- "What is our total revenue?"
- "Show me top 10 customers by sales"
- "What's the average DSO?"
- "Which customers have overdue payments?"
- "How many invoices were paid on time?"

---

## 🐛 Troubleshooting

### **Error: "depends on a node named ... which was not found"**

**Symptom:**
```
Model 'model.dbt_o2c_semantic.sv_o2c_customer_summary' depends on...
```

**Problem:** You're building from the **wrong project** (`dbt_o2c_semantic` instead of `dbt_o2c`)

**Solution:**
```bash
# Make sure you're in the RIGHT directory
cd O2C/dbt_o2c        # ✅ Correct

# NOT here:
cd O2C/dbt_o2c_semantic    # ❌ Wrong (deprecated)
```

### **Error: "Profile not found"**

**Solution:**
```bash
# Make sure profiles.yml exists
ls -la profiles.yml

# If missing, it should be in O2C/dbt_o2c/profiles.yml
```

### **Semantic views not created**

**Check:**
1. Did `dbt deps` install `dbt_semantic_view` package?
2. Check `dbt_packages/` folder exists
3. Re-run `dbt deps` if needed

---

## 📚 Additional Resources

- **Platform Overview**: `O2C_README.md`
- **Data Flow**: `O2C_DATA_FLOW_LINEAGE.md`
- **Monitoring Queries**: `O2C_MONITORING_QUERIES.md`
- **Dashboard Queries**: `O2C_DASHBOARD_QUERIES.md`
- **Setup Guide**: `O2C_SETUP_GUIDE.md`

---

## ✅ Success Checklist

- [ ] Pulled latest changes from git
- [ ] Navigated to `O2C/dbt_o2c` (NOT `dbt_o2c_semantic`)
- [ ] Ran `dbt deps` successfully
- [ ] Ran `dbt build` successfully
- [ ] All 10 models built (8 tables/views + 2 semantic views)
- [ ] All 25 tests passed
- [ ] Verified semantic views exist in `O2C_SEMANTIC_VIEWS` schema
- [ ] Tested querying a semantic view
- [ ] Connected Cortex Analyst to semantic views

---

**Need help?** Check `O2C/dbt_o2c_semantic/DEPRECATED_README.md` for migration notes.

