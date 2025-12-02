# dbt_o2c_semantic - O2C Semantic Views

**Project Type:** Snowflake Semantic Views (DYNAMIC TABLEs)  
**Purpose:** Business-friendly views with auto-refresh  
**Package:** [Snowflake-Labs/dbt_semantic_view](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view/latest/)  
**Data Flow:** Creates actual Snowflake DYNAMIC TABLE views

---

## 📊 Overview

This project uses **Snowflake's dbt_semantic_view package** to create business-friendly semantic views as **DYNAMIC TABLEs**. Unlike traditional semantic layers, these views create actual database objects that auto-refresh.

---

## 🎯 What's Included

**3 Semantic Views (DYNAMIC TABLEs):**

1. **`sv_o2c_summary`** - Aggregated O2C metrics by customer and period
2. **`sv_o2c_customer_metrics`** - Customer-centric analytics view
3. **`sv_o2c_ar_aging`** - AR aging analysis with standard buckets

All views auto-refresh every **1 hour** via Snowflake DYNAMIC TABLE.

---

## 🚀 Deploy

```bash
# Install the Snowflake package
dbt deps

# Build semantic views (creates DYNAMIC TABLEs in Snowflake)
dbt build

# Expected output:
# ✓ 3 dynamic tables created in O2C_SEMANTIC_VIEWS schema
```

---

## 📊 Query Semantic Views

```sql
-- Query the summary view
SELECT *
FROM EDW.O2C_SEMANTIC_VIEWS.SV_O2C_SUMMARY
WHERE customer_type = 'E'
  AND order_month >= '2024-01-01';

-- Query customer metrics
SELECT 
    customer_name,
    lifetime_order_value,
    current_ar_outstanding,
    avg_dso,
    on_time_payment_rate
FROM EDW.O2C_SEMANTIC_VIEWS.SV_O2C_CUSTOMER_METRICS
ORDER BY current_ar_outstanding DESC;

-- Query AR aging
SELECT 
    customer_name,
    current_amount,
    past_due_1_30_days,
    past_due_31_60_days,
    past_due_over_90_days,
    total_ar_outstanding
FROM EDW.O2C_SEMANTIC_VIEWS.SV_O2C_AR_AGING
WHERE total_ar_outstanding > 0
ORDER BY total_ar_outstanding DESC;
```

---

## 🔄 Auto-Refresh

All semantic views are **DYNAMIC TABLEs** with:
- **Target Lag:** 1 hour
- **Auto-refresh:** Managed by Snowflake
- **Incremental:** Only processes changed data

Check refresh history:
```sql
SHOW DYNAMIC TABLES LIKE 'SV_O2C%' IN SCHEMA EDW.O2C_SEMANTIC_VIEWS;
```

---

## 📝 Key Difference from Metadata-Only Semantic Layer

| Aspect | This Approach (Snowflake) | dbt Semantic Layer (MetricFlow) |
|--------|---------------------------|----------------------------------|
| **Creates Objects** | ✅ Yes (DYNAMIC TABLEs) | ❌ No (metadata only) |
| **Auto-Refresh** | ✅ Yes (Snowflake managed) | ❌ No |
| **Query Method** | Standard SQL | Metrics API |
| **Performance** | ✅ Pre-computed | On-demand |
| **Storage** | Uses warehouse storage | No storage |

---

For more details, see `O2C_README.md` in the parent folder.
