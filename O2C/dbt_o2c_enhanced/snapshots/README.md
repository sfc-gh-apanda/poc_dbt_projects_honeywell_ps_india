# Snapshots - SCD-2 Historical Tracking

## Overview

Snapshots capture historical changes to dimension data using **Slowly Changing Dimension Type 2 (SCD-2)** pattern with **soft delete** support.

## Key Features

| Feature | Description |
|---------|-------------|
| **Full History** | All changes preserved, nothing lost |
| **Soft Delete** | Deleted records marked, never removed |
| **Point-in-Time** | Query data as of any historical date |
| **Auto Columns** | dbt adds validity and tracking columns |

## Available Snapshots

| Snapshot | Source | Description |
|----------|--------|-------------|
| `snap_customer` | `DIM_CUSTOMER` | Customer master data history |

## Execution

### Run All Snapshots
```bash
# dbt Core
dbt snapshot

# Snowflake Native dbt
EXECUTE DBT PROJECT dbt_o2c_enhanced ARGS = 'snapshot';
```

### Run Specific Snapshot
```bash
# dbt Core
dbt snapshot --select snap_customer

# Snowflake Native dbt
EXECUTE DBT PROJECT dbt_o2c_enhanced ARGS = 'snapshot --select snap_customer';
```

### Full Pipeline (Recommended Order)
```bash
# 1. Run snapshots first (capture history)
dbt snapshot

# 2. Run models (use snapshot data)
dbt run

# 3. Run tests
dbt test
```

## Auto-Generated Columns

dbt automatically adds these columns to snapshot tables:

| Column | Description |
|--------|-------------|
| `dbt_scd_id` | Unique ID for each version |
| `dbt_valid_from` | When this version became active |
| `dbt_valid_to` | When this version ended (NULL = current) |
| `dbt_updated_at` | When snapshot last processed |

## Soft Delete Behavior

When `invalidate_hard_deletes=True`:

```
Day 1: Customer ABC exists in source
       → Snapshot captures: dbt_valid_to = NULL

Day 2: Customer ABC removed from source  
       → Snapshot updates: dbt_valid_to = timestamp
       → Record is NOT deleted, just closed
       
Result: Full audit trail preserved
```

## Query Patterns

### Current Records Only
```sql
SELECT * FROM snap_customer 
WHERE dbt_valid_to IS NULL;
```

### Point-in-Time Query
```sql
-- Customer data as of June 15, 2024
SELECT * FROM snap_customer 
WHERE '2024-06-15' BETWEEN dbt_valid_from 
  AND COALESCE(dbt_valid_to, '9999-12-31');
```

### Change History
```sql
-- All versions of a specific customer
SELECT * FROM snap_customer 
WHERE customer_num_sk = '12345'
ORDER BY dbt_valid_from;
```

## Control-M Integration

```
┌─────────────────────────────────────────────┐
│  IICS_LOAD_COMPLETE                         │
│         │                                   │
│         ▼                                   │
│  ┌─────────────────┐                       │
│  │ DBT_SNAPSHOT    │  Run snapshots first  │
│  └────────┬────────┘                       │
│           │                                 │
│           ▼                                 │
│  ┌─────────────────┐                       │
│  │ DBT_RUN         │  Then run models      │
│  └────────┬────────┘                       │
│           │                                 │
│           ▼                                 │
│  ┌─────────────────┐                       │
│  │ DBT_TEST        │  Finally run tests    │
│  └─────────────────┘                       │
└─────────────────────────────────────────────┘
```

## Configuration Reference

```sql
{% snapshot snap_example %}
{{
    config(
        target_database='EDW',
        target_schema='O2C_ENHANCED_SNAPSHOTS',
        unique_key='primary_key_column',
        
        -- Strategy options:
        strategy='check',           -- Compare columns
        -- OR
        -- strategy='timestamp',    -- Use timestamp column
        -- updated_at='load_ts',
        
        check_cols=['col1', 'col2'],  -- Columns to monitor
        
        invalidate_hard_deletes=True  -- Enable soft delete
    )
}}
SELECT ... FROM {{ source('schema', 'table') }}
{% endsnapshot %}
```

