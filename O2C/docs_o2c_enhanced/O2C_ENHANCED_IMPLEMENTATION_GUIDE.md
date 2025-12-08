# O2C Enhanced - Implementation Guide

**Version:** 2.0.0  
**Date:** December 8, 2025  
**Project:** dbt_o2c_enhanced

---

## 📋 Overview

This guide covers the implementation of the enhanced O2C platform with:

1. ✅ **Audit Columns & Watermarking** - Track every record's lineage
2. ✅ **Data Load Patterns** - 5 different patterns for various use cases
3. ✅ **Processing Tracking Tables** - Run and model-level logging
4. ✅ **Automated Scheduling** - Snowflake Tasks for dbt execution
5. ✅ **Enhanced Telemetry** - Row count tracking and validation

---

## 🚀 Quick Start (5 Minutes)

### Step 1: Run Setup Scripts (Snowflake)

```sql
-- Execute in Snowsight in this order:
1. @O2C_ENHANCED_AUDIT_SETUP.sql       -- Creates tracking tables
2. @O2C_ENHANCED_SCHEDULING_SETUP.sql  -- Creates scheduled tasks
3. @O2C_ENHANCED_TELEMETRY_SETUP.sql   -- Creates monitoring views
```

### Step 2: Deploy dbt Project

```
1. Navigate to Snowsight → Develop → dbt Projects
2. Click "Create Project"
3. Connect to Git repository
4. Select folder: O2C/dbt_o2c_enhanced
5. Click "Create"
```

### Step 3: First Build

```
1. Open dbt_o2c_enhanced project in Snowsight
2. Click "Build" button
3. Wait for completion (~2-5 minutes)
```

### Step 4: Verify

```sql
-- Check row counts
SELECT * FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING;

-- Check audit columns
SELECT * FROM EDW.O2C_AUDIT.V_AUDIT_COLUMN_VALIDATION;

-- Check run log
SELECT * FROM EDW.O2C_AUDIT.DBT_RUN_LOG ORDER BY run_started_at DESC;
```

---

## 📁 Project Structure

```
O2C/dbt_o2c_enhanced/
├── dbt_project.yml           # Main project config with hooks
├── profiles.yml              # Snowflake connection
├── packages.yml              # dbt_utils, dbt_expectations
│
├── macros/
│   ├── audit/
│   │   ├── audit_columns.sql     # Audit column macros
│   │   └── row_hash.sql          # Row hash for change detection
│   └── logging/
│       ├── log_run.sql           # Run start/end logging
│       └── log_model.sql         # Model execution logging
│
├── models/
│   ├── sources/
│   │   └── _sources.yml          # Source definitions
│   │
│   ├── staging/o2c/
│   │   ├── stg_enriched_orders.sql
│   │   ├── stg_enriched_invoices.sql
│   │   ├── stg_enriched_payments.sql
│   │   └── _stg_o2c.yml
│   │
│   └── marts/
│       ├── dimensions/
│       │   ├── dim_o2c_customer.sql      # Pattern 1: Truncate & Load
│       │   └── _dimensions.yml
│       │
│       ├── core/
│       │   └── dm_o2c_reconciliation.sql # Pattern 2: Merge (Upsert)
│       │
│       ├── events/
│       │   └── fact_o2c_events.sql       # Pattern 3: Append Only
│       │
│       ├── partitioned/
│       │   ├── fact_o2c_daily.sql        # Pattern 4: Delete+Insert
│       │   └── fact_o2c_by_source.sql    # Pattern 5: Pre-Hook Delete
│       │
│       └── aggregates/
│           └── agg_o2c_by_customer.sql
│
└── .gitignore
```

---

## 📊 Data Load Patterns

### Pattern 1: Truncate & Load (`materialized='table'`)

**Model:** `dim_o2c_customer.sql`  
**Use Case:** Dimension tables, small-medium tables  
**Behavior:** Full table replacement every run

```sql
{{ config(materialized='table') }}

SELECT 
    columns,
    {{ audit_columns() }}  -- Standard audit columns
FROM source
```

**Testing:**
```sql
-- Before run: Note dbt_loaded_at timestamps
-- After run: ALL records have NEW dbt_loaded_at
SELECT dbt_run_id, dbt_loaded_at, COUNT(*) 
FROM dim_o2c_customer 
GROUP BY 1, 2;
-- Expect: All records have same dbt_run_id and dbt_loaded_at
```

---

### Pattern 2: Merge / Upsert (`incremental_strategy='merge'`)

**Model:** `dm_o2c_reconciliation.sql`  
**Use Case:** Facts with updates, SCD Type 1  
**Behavior:** Insert new, Update existing

```sql
{{ config(
    materialized='incremental',
    unique_key='reconciliation_key',
    incremental_strategy='merge',
    merge_update_columns=['col1', 'col2', 'dbt_updated_at']
) }}

SELECT 
    columns,
    {{ audit_columns_incremental() }}  -- Preserves dbt_created_at
FROM source
{% if is_incremental() %}
WHERE change_detected
{% endif %}
```

**Testing:**
```sql
-- Run 1: Creates table
dbt run --select dm_o2c_reconciliation

-- Note timestamps
SELECT reconciliation_key, dbt_created_at, dbt_updated_at 
FROM dm_o2c_reconciliation LIMIT 5;

-- Run 2: After source changes
dbt run --select dm_o2c_reconciliation

-- Verify: dbt_created_at unchanged, dbt_updated_at updated
SELECT reconciliation_key, dbt_created_at, dbt_updated_at 
FROM dm_o2c_reconciliation 
WHERE dbt_created_at != dbt_updated_at;
```

---

### Pattern 3: Append Only (`incremental_strategy='append'`)

**Model:** `fact_o2c_events.sql`  
**Use Case:** Event logs, audit trails  
**Behavior:** Insert only, never update

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT columns
FROM source
{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

**Testing:**
```sql
-- Run 1: Creates table with initial events
dbt run --select fact_o2c_events

-- Count
SELECT COUNT(*), MAX(event_timestamp) FROM fact_o2c_events;

-- Add new events to source, then run again
dbt run --select fact_o2c_events

-- Verify: Old events unchanged, new events added
SELECT 
    dbt_run_id, 
    COUNT(*) as records,
    MIN(event_timestamp) as min_ts,
    MAX(event_timestamp) as max_ts
FROM fact_o2c_events
GROUP BY 1
ORDER BY min_ts;
-- Expect: Multiple run_ids, events never updated
```

---

### Pattern 4: Delete+Insert (`incremental_strategy='delete+insert'`)

**Model:** `fact_o2c_daily.sql`  
**Use Case:** Partition reloads, late-arriving data  
**Behavior:** Delete matching partition, Insert fresh

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    incremental_predicates=[
        "DBT_INTERNAL_DEST.order_date >= DATEADD('day', -3, CURRENT_DATE())"
    ]
) }}

SELECT columns
FROM source
WHERE order_date >= DATEADD('day', -{{ var('reload_days', 3) }}, CURRENT_DATE())
```

**Testing:**
```sql
-- Run 1: Creates table
dbt run --select fact_o2c_daily

-- Note record counts by date
SELECT order_date, COUNT(*), MAX(dbt_loaded_at) as loaded_at
FROM fact_o2c_daily
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;

-- Run 2: Reloads last 3 days
dbt run --select fact_o2c_daily

-- Verify: Last 3 days have new dbt_loaded_at, older data unchanged
SELECT order_date, COUNT(*), MAX(dbt_loaded_at) as loaded_at
FROM fact_o2c_daily
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
-- Expect: Recent dates have current timestamp, old dates unchanged
```

---

### Pattern 5: Pre-Hook Delete

**Model:** `fact_o2c_by_source.sql`  
**Use Case:** Source-specific reloads  
**Behavior:** Delete specific source, Insert fresh

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["DELETE FROM {{ this }} WHERE source_system = '{{ var('reload_source') }}'"]
) }}

SELECT columns
FROM source
WHERE source_system = '{{ var("reload_source") }}'
```

**Testing:**
```sql
-- Run 1: Load all sources
dbt run --select fact_o2c_by_source

-- Check by source
SELECT source_system, COUNT(*), MAX(dbt_loaded_at)
FROM fact_o2c_by_source
GROUP BY 1;

-- Run 2: Reload only BRP source
dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP"}'

-- Verify: BRP has new timestamp, others unchanged
SELECT source_system, COUNT(*), MAX(dbt_loaded_at)
FROM fact_o2c_by_source
GROUP BY 1;
```

---

## 🔍 Audit Columns Reference

### Standard Audit Columns (`{{ audit_columns() }}`)

| Column | Type | Description |
|--------|------|-------------|
| `dbt_run_id` | VARCHAR(50) | Unique dbt invocation ID |
| `dbt_batch_id` | VARCHAR(32) | Unique per model per run |
| `dbt_loaded_at` | TIMESTAMP_NTZ | When record was loaded |
| `dbt_source_model` | VARCHAR(100) | Model that created the record |
| `dbt_environment` | VARCHAR(20) | dev or prod |

### Incremental Audit Columns (`{{ audit_columns_incremental() }}`)

| Column | Type | Description |
|--------|------|-------------|
| `dbt_run_id` | VARCHAR(50) | Unique dbt invocation ID |
| `dbt_batch_id` | VARCHAR(32) | Unique per model per run |
| `dbt_created_at` | TIMESTAMP_NTZ | First insert time (preserved on update) |
| `dbt_updated_at` | TIMESTAMP_NTZ | Last modification time |
| `dbt_source_model` | VARCHAR(100) | Model that created the record |
| `dbt_environment` | VARCHAR(20) | dev or prod |

### Row Hash (`{{ row_hash(['col1', 'col2']) }}`)

| Column | Type | Description |
|--------|------|-------------|
| `dbt_row_hash` | VARCHAR(32) | MD5 hash for change detection |

---

## 📈 Monitoring & Verification

### Check Processing Status

```sql
-- Latest runs
SELECT * FROM EDW.O2C_AUDIT.V_DAILY_RUN_SUMMARY;

-- Recent failures
SELECT * FROM EDW.O2C_AUDIT.V_RECENT_FAILURES;

-- Model execution history
SELECT * FROM EDW.O2C_AUDIT.V_MODEL_EXECUTION_HISTORY;
```

### Validate Data Flow

```sql
-- Row count reconciliation
SELECT * FROM EDW.O2C_AUDIT.V_DATA_FLOW_VALIDATION;

-- All layer row counts
SELECT * FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING;

-- Audit column validation
SELECT * FROM EDW.O2C_AUDIT.V_AUDIT_COLUMN_VALIDATION;
```

### Load Pattern Analysis

```sql
-- Performance by load pattern
SELECT * FROM EDW.O2C_AUDIT.V_LOAD_PATTERN_ANALYSIS;

-- Batch tracking
SELECT * FROM EDW.O2C_AUDIT.V_BATCH_TRACKING;
```

---

## ⏰ Scheduling

### Default Schedule

| Task | Schedule | Description |
|------|----------|-------------|
| `O2C_DAILY_BUILD` | 6 AM UTC | Full dbt build |
| `O2C_POST_BUILD_TESTS` | After daily build | Run dbt tests |
| `O2C_PARTITION_RELOAD` | 7 AM UTC | Reload last 3 days |

### Manual Execution

```sql
-- Run daily build manually
EXECUTE TASK O2C_DAILY_BUILD;

-- Run specific model
CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt run --select dm_o2c_reconciliation');
```

### Task Management

```sql
-- Suspend a task
ALTER TASK O2C_DAILY_BUILD SUSPEND;

-- Resume a task
ALTER TASK O2C_DAILY_BUILD RESUME;

-- Check task status
SHOW TASKS IN SCHEMA EDW.O2C_AUDIT;
```

---

## 🧪 Testing Checklist

### After Initial Setup

- [ ] All setup SQL scripts executed successfully
- [ ] dbt_o2c_enhanced project recognized in Snowsight
- [ ] First dbt build completes without errors

### Audit Columns

- [ ] `dim_o2c_customer` has all audit columns populated
- [ ] `dm_o2c_reconciliation` preserves `dbt_created_at` on updates
- [ ] `V_AUDIT_COLUMN_VALIDATION` shows ✅ VALID for all models

### Data Load Patterns

- [ ] **Truncate & Load:** All records have same `dbt_loaded_at`
- [ ] **Merge:** `dbt_created_at` preserved, `dbt_updated_at` current
- [ ] **Append:** Old events unchanged, new events appended
- [ ] **Delete+Insert:** Only recent partitions refreshed
- [ ] **Pre-Hook Delete:** Only specified source refreshed

### Processing Tracking

- [ ] `DBT_RUN_LOG` has entries for each dbt run
- [ ] `DBT_MODEL_LOG` has entries for each model
- [ ] Run status (SUCCESS/FAILED) is accurate

### Scheduling

- [ ] Tasks created in Snowflake
- [ ] Daily build task enabled
- [ ] Task execution logged

---

## 🔧 Troubleshooting

### "Audit tables not found"

```sql
-- Run the audit setup script
@O2C_ENHANCED_AUDIT_SETUP.sql
```

### "Model failed with contract error"

```
-- Check that audit columns match schema contract in YAML
-- Ensure all columns in SELECT match the contract definition
```

### "No data in monitoring views"

```sql
-- Views require at least one dbt run
-- Execute a build first:
CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt build');
```

### "Task not running"

```sql
-- Check task state
SHOW TASKS LIKE 'O2C%';

-- Resume if suspended
ALTER TASK O2C_DAILY_BUILD RESUME;
```

---

## 📚 Related Files

| File | Purpose |
|------|---------|
| `O2C_ENHANCED_AUDIT_SETUP.sql` | Creates tracking tables |
| `O2C_ENHANCED_SCHEDULING_SETUP.sql` | Creates Snowflake tasks |
| `O2C_ENHANCED_TELEMETRY_SETUP.sql` | Creates monitoring views |
| `dbt_o2c_enhanced/dbt_project.yml` | Project configuration |
| `dbt_o2c_enhanced/macros/audit/` | Audit column macros |
| `dbt_o2c_enhanced/macros/logging/` | Logging macros |

---

## ✅ Summary

The `dbt_o2c_enhanced` project demonstrates:

1. **5 Data Load Patterns** with real-world examples
2. **Comprehensive Audit Columns** for full traceability
3. **Automated Processing Tracking** via hooks
4. **Snowflake Task Scheduling** for automation
5. **Enhanced Telemetry** for monitoring and validation

This serves as a reference implementation for enterprise dbt deployments.


