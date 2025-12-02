# dbt_o2c - Order-to-Cash Data Platform

**Project Type:** dbt Data Platform  
**Purpose:** Staging, dimensions, and marts for O2C analytics  
**Database:** Snowflake (EDW)

---

## 📊 Project Overview

This dbt project creates the complete data platform for Order-to-Cash analytics, including:
- **Staging Layer**: Enriched staging models with dimension joins
- **Dimension Layer**: Shared dimensions (customer, payment terms, bank)
- **Mart Layer**: Core facts, reconciliation, and pre-aggregated summaries

---

## 🏗️ Architecture

```
dbt_o2c
├── Staging (schema: o2c_staging)
│   ├── stg_enriched_orders      (view with customer join)
│   ├── stg_enriched_invoices    (view with payment terms join)
│   └── stg_enriched_payments    (view with bank join)
│
├── Dimensions (schema: o2c_dimensions)
│   ├── dim_o2c_customer
│   ├── dim_o2c_payment_terms
│   └── dim_o2c_bank
│
├── Core Marts (schema: o2c_core)
│   ├── fct_o2c_transactions
│   ├── dm_o2c_reconciliation    ← Main mart
│   └── dm_o2c_cycle_analysis
│
└── Aggregates (schema: o2c_aggregates)
    ├── agg_o2c_by_customer
    ├── agg_o2c_by_period
    └── agg_o2c_performance
```

---

## 🚀 Quick Start

```bash
# Install dependencies
dbt deps

# Build all models
dbt build

# Build specific layer
dbt build --select staging
dbt build --select marts
```

---

## 📊 Data Flow

```
Sources (6 tables)
    ↓
Staging (3 models with joins)
    ↓
Marts (9 models)
    ↓
Output: 12 warehouse objects
```

---

## 🧪 Testing

```bash
# Run all tests
dbt test

# Test specific models
dbt test --select staging
dbt test --select dm_o2c_reconciliation
```

---

## 📚 Documentation

```bash
# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve
```

---

## 🔧 Configuration

- **Target Database:** EDW
- **Schemas:** o2c_staging, o2c_dimensions, o2c_core, o2c_aggregates
- **Materialization:** Views (staging), Tables (marts)
- **Access:** Public (for semantic layer)

---

For more details, see `O2C_README.md` in the parent folder.

