# dbt_o2c_semantic - O2C Semantic Layer

**Project Type:** dbt Semantic Layer (Metadata Only)  
**Purpose:** Business metrics and semantic models  
**Data Flow:** None (YAML definitions only)

---

## 📊 Overview

This project contains **metadata-only definitions** for the O2C semantic layer. It does not create any database objects - only defines how business users can query O2C data through metrics.

---

## 🎯 What's Included

- **1 Semantic Model:** sm_o2c_reconciliation
- **13+ Metrics:** DSO, collection rate, cycle times, etc.
- **10+ Dimensions:** Customer, time, status categories
- **8+ Measures:** Counts, amounts, averages

---

## 🚀 Deploy

```bash
# Parse semantic models (no data created)
dbt parse

# Deploy to dbt Cloud Semantic Layer
dbt cloud deploy-semantic-layer
```

---

## 📊 Query Metrics

```bash
# Via dbt Semantic Layer
dbt sl query \
  --metrics mtc_days_sales_outstanding \
  --group-by customer_type

# Via MetricFlow
mf query \
  --metrics mtc_cash_collected \
  --group-by metric_time__month
```

---

## 📝 Note

This project creates **NO warehouse objects**. All metrics query existing tables from `dbt_o2c` project.

---

For more details, see `O2C_README.md` in the parent folder.

