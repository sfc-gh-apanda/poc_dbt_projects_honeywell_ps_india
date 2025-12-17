# O2C Enhanced - Data Load Patterns Testing Guide

**Version:** 2.0.0  
**Date:** December 2024  
**Purpose:** Step-by-step guide to test and demonstrate all 5 data load patterns

---

## 📋 Pre-Requisites Checklist

Before testing, ensure the following are complete:

| Step | Script/Action | Status |
|------|---------------|--------|
| 1 | Run `O2C_ENHANCED_AUDIT_SETUP.sql` | ☐ |
| 2 | Run `O2C_ENHANCED_DYNAMIC_WAREHOUSE_SETUP.sql` | ☐ |
| 3 | Run `O2C_ENHANCED_SAMPLE_DATA_SETUP.sql` | ☐ |
| 4 | Deploy dbt project in Snowsight | ☐ |

---

## 🚀 Quick Start: Complete Test Run

```sql
-- Step 1: Verify source data exists
SELECT 'FACT_SALES_ORDERS' AS tbl, COUNT(*) AS cnt FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
UNION ALL SELECT 'FACT_INVOICES', COUNT(*) FROM EDW.CORP_TRAN.FACT_INVOICES
UNION ALL SELECT 'FACT_PAYMENTS', COUNT(*) FROM EDW.CORP_TRAN.FACT_PAYMENTS;

-- Step 2: Run dbt build (in Snowsight dbt IDE or CLI)
-- dbt build

-- Step 3: Verify all output tables created
SELECT * FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING;
```

---

## 📊 Pattern 1: Truncate & Load (dim_o2c_customer)

**Model:** `dim_o2c_customer.sql`  
**Materialization:** `table`  
**Behavior:** Complete table replacement on every run

### Before Run - Capture Baseline

```sql
-- BEFORE: Capture current state
SELECT 
    'BEFORE RUN' AS snapshot_type,
    CURRENT_TIMESTAMP() AS captured_at,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT dbt_run_id) AS distinct_run_ids,
    MIN(dbt_loaded_at) AS earliest_load,
    MAX(dbt_loaded_at) AS latest_load
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER;

-- BEFORE: Sample records with audit columns
SELECT 
    customer_key,
    customer_name,
    source_system,
    dbt_run_id,
    dbt_loaded_at,
    dbt_batch_id
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER
LIMIT 5;
```

### Execute Run

```bash
# CLI
dbt run --select dim_o2c_customer

# OR in Snowsight: Click "Run" on dim_o2c_customer model
```

### After Run - Verify Results

```sql
-- AFTER: Capture new state
SELECT 
    'AFTER RUN' AS snapshot_type,
    CURRENT_TIMESTAMP() AS captured_at,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT dbt_run_id) AS distinct_run_ids,
    MIN(dbt_loaded_at) AS earliest_load,
    MAX(dbt_loaded_at) AS latest_load
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER;

-- AFTER: Verify ALL records have NEW timestamps
SELECT 
    dbt_run_id,
    dbt_loaded_at,
    COUNT(*) AS row_count
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER
GROUP BY 1, 2
ORDER BY dbt_loaded_at DESC;
```

### ✅ Expected Result

| Metric | Expected |
|--------|----------|
| `distinct_run_ids` | 1 (all records have same run_id) |
| `dbt_loaded_at` | All records have current timestamp |
| Row count | Same as source |

---

## 📊 Pattern 2: Merge/Upsert (dm_o2c_reconciliation)

**Model:** `dm_o2c_reconciliation.sql`  
**Materialization:** `incremental` with `merge` strategy  
**Behavior:** Insert new records, Update existing records

### Before Run - Capture Baseline

```sql
-- BEFORE: Capture baseline
SELECT 
    'BEFORE RUN' AS snapshot_type,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN dbt_created_at = dbt_updated_at THEN 1 END) AS never_updated,
    COUNT(CASE WHEN dbt_created_at != dbt_updated_at THEN 1 END) AS updated_rows
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION;

-- BEFORE: Sample showing created vs updated timestamps
SELECT 
    reconciliation_key,
    source_system,
    dbt_created_at,
    dbt_updated_at,
    CASE WHEN dbt_created_at = dbt_updated_at THEN 'NEVER UPDATED' ELSE 'UPDATED' END AS update_status
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
LIMIT 10;
```

### Execute Run

```bash
dbt run --select dm_o2c_reconciliation
```

### After Run - Verify Results

```sql
-- AFTER: Verify created_at preserved, updated_at changed
SELECT 
    'AFTER RUN' AS snapshot_type,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN dbt_created_at = dbt_updated_at THEN 1 END) AS never_updated,
    COUNT(CASE WHEN dbt_created_at != dbt_updated_at THEN 1 END) AS updated_rows
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION;

-- AFTER: Show records that were updated (created_at preserved!)
SELECT 
    reconciliation_key,
    dbt_created_at,
    dbt_updated_at,
    DATEDIFF('second', dbt_created_at, dbt_updated_at) AS seconds_between
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
WHERE dbt_created_at != dbt_updated_at
LIMIT 10;
```

### ✅ Expected Result

| Metric | Expected |
|--------|----------|
| `dbt_created_at` | PRESERVED from first insert |
| `dbt_updated_at` | Current timestamp for updated rows |
| New rows | `created_at = updated_at` |

---

## 📊 Pattern 3: Append Only (fact_o2c_events)

**Model:** `fact_o2c_events.sql`  
**Materialization:** `incremental` with `append` strategy  
**Behavior:** Only insert new events, never update existing

### Before Run - Capture Baseline

```sql
-- BEFORE: Capture event counts by run_id
SELECT 
    'BEFORE RUN' AS snapshot_type,
    dbt_run_id,
    COUNT(*) AS event_count,
    MIN(event_timestamp) AS earliest_event,
    MAX(event_timestamp) AS latest_event
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS
GROUP BY dbt_run_id
ORDER BY earliest_event;

-- BEFORE: Total event count
SELECT COUNT(*) AS total_events FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS;
```

### Execute Run

```bash
dbt run --select fact_o2c_events
```

### After Run - Verify Results

```sql
-- AFTER: Show events by run_id (should see NEW run_id added)
SELECT 
    'AFTER RUN' AS snapshot_type,
    dbt_run_id,
    COUNT(*) AS event_count,
    MIN(event_timestamp) AS earliest_event,
    MAX(event_timestamp) AS latest_event
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS
GROUP BY dbt_run_id
ORDER BY earliest_event;

-- AFTER: Verify old events unchanged (same run_ids exist)
SELECT 
    COUNT(DISTINCT dbt_run_id) AS distinct_run_ids,
    COUNT(*) AS total_events
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS;
```

### ✅ Expected Result

| Metric | Expected |
|--------|----------|
| Old events | Unchanged (original run_id preserved) |
| New events | Added with new run_id |
| Total events | Increased (if new source data) |

---

## 📊 Pattern 4: Delete+Insert by Date (fact_o2c_daily)

**Model:** `fact_o2c_daily.sql`  
**Materialization:** `incremental` with `delete+insert` strategy  
**Behavior:** Delete and reload last N days (configurable)

### Before Run - Capture Baseline

```sql
-- BEFORE: Row counts by date (critical for validation!)
SELECT 
    'BEFORE RUN' AS snapshot_type,
    order_date,
    COUNT(*) AS row_count,
    MAX(dbt_loaded_at) AS loaded_at
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY
GROUP BY order_date
ORDER BY order_date DESC
LIMIT 15;

-- BEFORE: Save specific timestamps for comparison
CREATE OR REPLACE TEMPORARY TABLE _BEFORE_DAILY_SNAPSHOT AS
SELECT order_date, dbt_run_id, dbt_loaded_at, COUNT(*) AS cnt
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY
GROUP BY 1, 2, 3;
```

### Execute Run (Default: Last 3 Days)

```bash
# Default reload window (3 days)
dbt run --select fact_o2c_daily

# OR: Custom reload window (7 days)
dbt run --select fact_o2c_daily --vars '{"reload_days": 7}'
```

### After Run - Verify Results

```sql
-- AFTER: Compare before/after by date
SELECT 
    COALESCE(a.order_date, b.order_date) AS order_date,
    b.loaded_at AS before_loaded_at,
    a.loaded_at AS after_loaded_at,
    CASE 
        WHEN a.loaded_at > b.loaded_at THEN '🔄 RELOADED'
        WHEN a.loaded_at = b.loaded_at THEN '✅ UNCHANGED'
        ELSE '🆕 NEW'
    END AS status
FROM (
    SELECT order_date, MAX(dbt_loaded_at) AS loaded_at
    FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY
    GROUP BY 1
) a
FULL OUTER JOIN _BEFORE_DAILY_SNAPSHOT b ON a.order_date = b.order_date
ORDER BY order_date DESC
LIMIT 15;

-- AFTER: Verify only recent dates were reloaded
SELECT 
    CASE 
        WHEN order_date >= DATEADD('day', -3, CURRENT_DATE()) THEN 'WITHIN_RELOAD_WINDOW'
        ELSE 'OUTSIDE_RELOAD_WINDOW'
    END AS date_category,
    COUNT(DISTINCT dbt_run_id) AS distinct_run_ids,
    MAX(dbt_loaded_at) AS max_loaded_at
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY
GROUP BY 1;
```

### ✅ Expected Result

| Date Range | Expected Behavior |
|------------|-------------------|
| Last 3 days | DELETED and RE-INSERTED (new timestamps) |
| Older dates | UNCHANGED (original timestamps) |

---

## 📊 Pattern 5: Pre-Hook Delete by Source (fact_o2c_by_source)

**Model:** `fact_o2c_by_source.sql`  
**Materialization:** `incremental` with `append` + `pre_hook` delete  
**Behavior:** Delete specific source system, reload fresh data

### ⭐ THIS IS THE KEY PATTERN FOR REGIONAL DATA RELOADS

### Before Run - Capture Baseline

```sql
-- BEFORE: Row counts by source system (critical!)
SELECT 
    'BEFORE RUN' AS snapshot_type,
    source_system,
    COUNT(*) AS row_count,
    MAX(dbt_loaded_at) AS last_loaded_at,
    MAX(dbt_run_id) AS last_run_id
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_BY_SOURCE
GROUP BY source_system
ORDER BY source_system;

-- BEFORE: Save for comparison
CREATE OR REPLACE TEMPORARY TABLE _BEFORE_SOURCE_SNAPSHOT AS
SELECT source_system, dbt_run_id, dbt_loaded_at, COUNT(*) AS cnt
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_BY_SOURCE
GROUP BY 1, 2, 3;
```

### Scenario: BRP (North America) Data Was Wrong - Need to Reload

```bash
# ONLY reload BRP data - leave CIP and SAP untouched
dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP"}'
```

### After Run - Verify Results

```sql
-- AFTER: Compare before/after by source
SELECT 
    a.source_system,
    b.last_loaded_at AS before_loaded_at,
    a.last_loaded_at AS after_loaded_at,
    b.row_count AS before_count,
    a.row_count AS after_count,
    CASE 
        WHEN a.last_loaded_at > b.last_loaded_at THEN '🔄 RELOADED'
        ELSE '✅ UNCHANGED'
    END AS status
FROM (
    SELECT source_system, MAX(dbt_loaded_at) AS last_loaded_at, COUNT(*) AS row_count
    FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_BY_SOURCE
    GROUP BY 1
) a
LEFT JOIN (
    SELECT source_system, MAX(dbt_loaded_at) AS last_loaded_at, SUM(cnt) AS row_count
    FROM _BEFORE_SOURCE_SNAPSHOT
    GROUP BY 1
) b ON a.source_system = b.source_system
ORDER BY a.source_system;

-- AFTER: Verify distinct run_ids per source
SELECT 
    source_system,
    COUNT(DISTINCT dbt_run_id) AS distinct_run_ids,
    LISTAGG(DISTINCT dbt_run_id, ', ') AS run_ids
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_BY_SOURCE
GROUP BY source_system
ORDER BY source_system;
```

### ✅ Expected Result

| Source | Expected Behavior |
|--------|-------------------|
| **BRP** | 🔄 RELOADED (new dbt_loaded_at, new dbt_run_id) |
| **CIP** | ✅ UNCHANGED (original timestamps preserved) |
| **SAP** | ✅ UNCHANGED (original timestamps preserved) |

---

## 🧪 Complete Demo Scenario: Bad Data Fix

### Scenario

> "Europe (SAP) sent incorrect order amounts. We need to fix the source data and reload only SAP records without affecting North America (BRP) or Asia Pacific (CIP)."

### Step 1: Capture Before State

```sql
-- Save current state
SELECT source_system, COUNT(*) AS orders, SUM(order_amount) AS total_amount, MAX(dbt_loaded_at) AS loaded_at
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_BY_SOURCE
GROUP BY source_system;

-- Result:
-- BRP | 40 | 1,234,567.00 | 2024-12-17 10:00:00
-- CIP | 35 | 2,345,678.00 | 2024-12-17 10:00:00
-- SAP | 30 | 1,567,890.00 | 2024-12-17 10:00:00  <-- Wrong amounts!
```

### Step 2: Fix Source Data

```sql
-- Simulate fixing the SAP source data (10% correction)
UPDATE EDW.CORP_TRAN.FACT_SALES_ORDERS
SET ORDER_AMOUNT = ORDER_AMOUNT * 1.10  -- Fix 10% error
WHERE SOURCE_SYSTEM = 'SAP';
```

### Step 3: Reload Only SAP Data

```bash
dbt run --select fact_o2c_by_source --vars '{"reload_source": "SAP"}'
```

### Step 4: Verify After State

```sql
-- Check new state
SELECT source_system, COUNT(*) AS orders, SUM(order_amount) AS total_amount, MAX(dbt_loaded_at) AS loaded_at
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_BY_SOURCE
GROUP BY source_system;

-- Result:
-- BRP | 40 | 1,234,567.00 | 2024-12-17 10:00:00  <-- UNCHANGED ✅
-- CIP | 35 | 2,345,678.00 | 2024-12-17 10:00:00  <-- UNCHANGED ✅
-- SAP | 30 | 1,724,679.00 | 2024-12-17 14:30:00  <-- RELOADED with fixed data! 🔄
```

---

## 📈 Monitoring & Audit Queries

### Check Run History

```sql
-- All recent dbt runs
SELECT * FROM EDW.O2C_AUDIT.DBT_RUN_LOG 
ORDER BY run_started_at DESC 
LIMIT 10;
```

### Check Model Execution History

```sql
-- Model execution for fact_o2c_by_source
SELECT 
    run_id,
    model_name,
    status,
    started_at,
    execution_seconds,
    rows_affected
FROM EDW.O2C_AUDIT.DBT_MODEL_LOG
WHERE model_name = 'fact_o2c_by_source'
ORDER BY started_at DESC
LIMIT 10;
```

### Audit Column Validation

```sql
-- Verify all tables have proper audit columns
SELECT * FROM EDW.O2C_AUDIT.V_AUDIT_COLUMN_VALIDATION;
```

---

## 🎯 Summary: Commands Reference

| Pattern | Model | dbt Command |
|---------|-------|-------------|
| **1. Truncate & Load** | dim_o2c_customer | `dbt run --select dim_o2c_customer` |
| **2. Merge/Upsert** | dm_o2c_reconciliation | `dbt run --select dm_o2c_reconciliation` |
| **3. Append Only** | fact_o2c_events | `dbt run --select fact_o2c_events` |
| **4. Delete+Insert by Date** | fact_o2c_daily | `dbt run --select fact_o2c_daily --vars '{"reload_days": 7}'` |
| **5. Delete by Source** | fact_o2c_by_source | `dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP"}'` |

### Full Build

```bash
dbt build  # Runs all models + tests
```

### Build by Tag

```bash
dbt run --select tag:truncate_load     # Pattern 1
dbt run --select tag:merge             # Pattern 2
dbt run --select tag:append_only       # Pattern 3
dbt run --select tag:delete_insert     # Pattern 4
dbt run --select tag:pre_hook_delete   # Pattern 5
```

---

## ✅ Checklist: Testing Complete

- [ ] Pattern 1: Truncate & Load verified
- [ ] Pattern 2: Merge/Upsert verified (created_at preserved)
- [ ] Pattern 3: Append Only verified (old events unchanged)
- [ ] Pattern 4: Delete+Insert by Date verified (only recent dates reloaded)
- [ ] Pattern 5: Pre-Hook Delete by Source verified (only specified source reloaded)
- [ ] Audit columns populated correctly
- [ ] Run/Model logs captured in audit tables

