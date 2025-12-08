# dbt_o2c_enhanced

**Order-to-Cash Analytics with Full Audit & Telemetry**

## 🚀 Features

- ✅ **Audit Columns** - `dbt_run_id`, `dbt_batch_id`, `dbt_created_at`, `dbt_updated_at`
- ✅ **Row Hash** - Change detection via `dbt_row_hash`
- ✅ **5 Data Load Patterns** - Truncate, Append, Merge, Delete+Insert, Pre-Hook
- ✅ **Processing Tracking** - Run and model-level logging
- ✅ **Automated Logging** - Via dbt hooks

## 📁 Project Structure

```
dbt_o2c_enhanced/
├── models/
│   ├── staging/o2c/          # Enriched staging views
│   └── marts/
│       ├── dimensions/       # Pattern 1: Truncate & Load
│       ├── core/             # Pattern 2: Merge (Upsert)
│       ├── events/           # Pattern 3: Append Only
│       ├── partitioned/      # Patterns 4 & 5: Delete+Insert
│       └── aggregates/       # Summary tables
└── macros/
    ├── audit/                # Audit column macros
    └── logging/              # Processing tracking macros
```

## 🔧 Prerequisites

```sql
-- Run setup scripts first:
@O2C_ENHANCED_AUDIT_SETUP.sql
@O2C_ENHANCED_SCHEDULING_SETUP.sql
@O2C_ENHANCED_TELEMETRY_SETUP.sql
```

## 📊 Data Load Patterns

| Pattern | Model | Config |
|---------|-------|--------|
| Truncate & Load | `dim_o2c_customer` | `materialized='table'` |
| Merge (Upsert) | `dm_o2c_reconciliation` | `incremental_strategy='merge'` |
| Append Only | `fact_o2c_events` | `incremental_strategy='append'` |
| Delete+Insert | `fact_o2c_daily` | `incremental_strategy='delete+insert'` |
| Pre-Hook Delete | `fact_o2c_by_source` | `pre_hook` + `append` |

## 🏃 Running the Project

```bash
# Full build
dbt build

# Specific pattern
dbt run --select tag:merge

# With variables
dbt run --select fact_o2c_daily --vars '{"reload_days": 7}'
```

## 📚 Documentation

See `O2C_ENHANCED_IMPLEMENTATION_GUIDE.md` for complete documentation.


