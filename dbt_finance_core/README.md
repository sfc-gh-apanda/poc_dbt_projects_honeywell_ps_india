# dbt_finance_core

Finance Core domain dbt project - part of the Honeywell PoC implementation.

## Overview

This project contains finance-specific data models that depend on the `dbt_foundation` project for shared dimensions, staging models, and macros.

## Project Structure

```
dbt_finance_core/
├── dbt_project.yml       # Project configuration
├── dependencies.yml      # Cross-project dependencies (dbt_foundation)
├── packages.yml          # Hub package dependencies (dbt_utils, etc.)
├── profiles.yml          # Sample profile (copy to ~/.dbt/)
├── models/
│   └── marts/
│       └── finance/
│           ├── _finance.yml              # Model documentation & tests
│           └── dm_fin_ar_aging_simple.sql # AR Aging data mart
├── macros/               # Project-specific macros
├── seeds/                # Static reference data
├── tests/                # Custom singular tests
├── snapshots/            # SCD Type 2 snapshots
└── analyses/             # Ad-hoc analytical queries
```

## Dependencies

This project depends on:
- **dbt_foundation** - Shared dimensions, staging models, and macros
- **dbt_utils** - Utility macros from dbt Labs
- **dbt_expectations** - Data quality testing

## Models

### Data Marts

| Model | Description |
|-------|-------------|
| `dm_fin_ar_aging_simple` | Simplified AR aging data mart with aging buckets |

## Setup

### 1. Install Dependencies

```bash
dbt deps
```

### 2. Configure Profile

Copy `profiles.yml` to `~/.dbt/profiles.yml` and update with your Snowflake credentials:

```bash
cp profiles.yml ~/.dbt/profiles.yml
```

Or set environment variables:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`

### 3. Verify Configuration

```bash
dbt debug
```

### 4. Build Models

```bash
# Build all models
dbt build

# Build specific model
dbt build --select dm_fin_ar_aging_simple

# Build with dependencies from foundation
dbt build --select +dm_fin_ar_aging_simple
```

## Testing

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select dm_fin_ar_aging_simple
```

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `snapshot_date` | `CURRENT_DATE()` | Date for aging calculations |

Override at runtime:
```bash
dbt build --vars '{"snapshot_date": "2024-01-31"}'
```

## Cross-Project References

This project uses dbt's cross-project ref syntax to access foundation models:

```sql
-- Reference foundation staging model
select * from {{ ref('dbt_foundation', 'stg_ar_invoice') }}

-- Reference foundation dimension
select * from {{ ref('dbt_foundation', 'dim_customer') }}

-- Use foundation macro
{{ aging_bucket('days_late') }}
```

## Contact

For questions or issues, contact the Data Engineering team.

