# O2C Data Flow & Lineage Guide

**Project:** Honeywell O2C Analytics Platform  
**Date:** December 2, 2025  
**Status:** ✅ Production Ready

---

## 📊 Overview

Complete data lineage from **6 source tables** through **2 transformation layers** to produce **8 business-ready O2C analytics models**.

### Quick Summary

| Aspect | Details |
|--------|---------|
| **Source Tables** | 6 tables (3 facts + 3 dimensions) |
| **Transformation Layers** | 2 layers (Staging with joins → Marts) |
| **Staging Models** | 3 models (each with 1 LEFT JOIN) |
| **Mart Models** | 5 models (1 dimension + 2 core + 2 aggregates) |
| **Total Models** | 8 dbt models (3 staging + 5 marts) |
| **dbt Projects** | 1 project (dbt_o2c with semantic views) |
| **Semantic Views** | 2 Snowflake Semantic Views for Cortex Analyst |

---

## 🔄 Complete Data Flow

```
📁 SOURCE LAYER (6 Tables)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ FACT_SALES_      │  │ FACT_INVOICES    │  │ FACT_PAYMENTS    │
│ ORDERS           │  │                  │  │                  │
│ (CORP_TRAN)      │  │ (CORP_TRAN)      │  │ (CORP_TRAN)      │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                     │                      │
         │                     │                      │
         ▼                     ▼                      ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ DIM_CUSTOMER     │  │ DIM_PAYMENT_     │  │ DIM_BANK_        │
│                  │  │ TERMS            │  │ ACCOUNT          │
│ (CORP_MASTER)    │  │ (CORP_MASTER)    │  │ (CORP_MASTER)    │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                     │                      │
         │                     │                      │
         ▼                     ▼                      ▼

🔧 STAGING LAYER (3 Models with JOINS) - dbt_o2c
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌──────────────────────────────────────────────────────────┐
│ STG_ENRICHED_ORDERS (VIEW)                               │
│ = FACT_SALES_ORDERS + DIM_CUSTOMER                       │
│ JOIN: customer_id + source_system                        │
└────────┬─────────────────────────────────────────────────┘
         │
┌──────────────────────────────────────────────────────────┐
│ STG_ENRICHED_INVOICES (VIEW)                             │
│ = FACT_INVOICES + DIM_PAYMENT_TERMS                      │
│ JOIN: payment_terms_code + source_system                 │
└────────┬─────────────────────────────────────────────────┘
         │
┌──────────────────────────────────────────────────────────┐
│ STG_ENRICHED_PAYMENTS (VIEW)                             │
│ = FACT_PAYMENTS + DIM_BANK_ACCOUNT                       │
│ JOIN: bank_account_id + source_system                    │
└────────┬─────────────────────────────────────────────────┘
         │
         ▼

📊 MART LAYER (5 Models) - dbt_o2c
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌──────────────────────────────────────────────────────────┐
│ DIM_O2C_CUSTOMER (TABLE) - Published Dimension           │
│ = DIM_CUSTOMER (source)                                  │
│ Schema contract enforced, access: public                 │
└──────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────┐
│ DM_O2C_RECONCILIATION (TABLE) - Main Core Mart            │
│ = STG_ENRICHED_ORDERS + STG_ENRICHED_INVOICES +           │
│   STG_ENRICHED_PAYMENTS                                   │
│ JOINs: order_key, invoice_key                             │
│ Output: Complete O2C view with all enriched data          │
└─────────────────────────┬─────────────────────────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
┌─────────────────┐  ┌──────────────┐  ┌─────────────────┐
│ DM_O2C_CYCLE_   │  │ AGG_O2C_BY_  │  │ AGG_O2C_BY_     │
│ ANALYSIS        │  │ CUSTOMER     │  │ PERIOD          │
│ (TABLE)         │  │ (TABLE)      │  │ (TABLE)         │
│ Cycle metrics   │  │ Customer agg │  │ Time-series agg │
└─────────────────┘  └──────────────┘  └─────────────────┘

🎯 SEMANTIC LAYER (2 Snowflake Semantic Views) - dbt_o2c
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌───────────────────────────────────────────────────────────┐
│ Semantic Views (for Cortex Analyst)                       │
│ - sv_o2c_reconciliation                                   │
│ - sv_o2c_customer_summary                                 │
│ - Uses dbt_semantic_view package (Snowflake-Labs)         │
│ - Created as SEMANTIC VIEW objects in Snowflake           │
└───────────────────────────────────────────────────────────┘
```

---

## 🔗 Join Relationships

### **Staging Layer Joins (3 joins)**

```
1. FACT_SALES_ORDERS 
   LEFT JOIN DIM_CUSTOMER
   ON customer_id = customer_num_sk AND source_system

2. FACT_INVOICES
   LEFT JOIN DIM_PAYMENT_TERMS
   ON payment_terms_code AND source_system

3. FACT_PAYMENTS
   LEFT JOIN DIM_BANK_ACCOUNT
   ON bank_account_id AND source_system
```

### **Mart Layer Joins (2 joins)**

```
DM_O2C_RECONCILIATION:
  FROM stg_enriched_orders
  LEFT JOIN stg_enriched_invoices
    ON order_key
  LEFT JOIN stg_enriched_payments
    ON invoice_key
```

**Total Joins:** 5 (3 in staging + 2 in mart)

---

## 📦 Complete Model Inventory

### **Staging Models (3 views)**
1. `stg_enriched_orders` - Orders + Customer (LEFT JOIN)
2. `stg_enriched_invoices` - Invoices + Payment Terms (LEFT JOIN)
3. `stg_enriched_payments` - Payments + Bank Account (LEFT JOIN)

### **Mart Models (5 tables)**

**Dimensions (1):**
1. `dim_o2c_customer` - Published customer dimension (schema contract enforced)

**Core Marts (2):**
1. `dm_o2c_reconciliation` - Main O2C reconciliation (joins all 3 staging)
2. `dm_o2c_cycle_analysis` - Cycle time analysis (completed transactions only)

**Aggregates (2):**
1. `agg_o2c_by_customer` - Customer-level summary metrics
2. `agg_o2c_by_period` - Time-series monthly aggregations

**Total: 8 dbt models**

---

## 📈 Data Volume (Sample Data)

| Layer | Model | Rows | Type |
|-------|-------|------|------|
| Source | FACT_SALES_ORDERS | ~100 | Fact |
| Source | FACT_INVOICES | ~80 | Fact |
| Source | FACT_PAYMENTS | ~60 | Fact |
| Source | DIM_CUSTOMER | ~10 | Dimension |
| Staging | STG_ENRICHED_ORDERS | ~100 | View |
| Staging | STG_ENRICHED_INVOICES | ~80 | View |
| Staging | STG_ENRICHED_PAYMENTS | ~60 | View |
| Mart - Dimension | DIM_O2C_CUSTOMER | ~10 | Table |
| Mart - Core | DM_O2C_RECONCILIATION | ~100 | Table |
| Mart - Core | DM_O2C_CYCLE_ANALYSIS | ~60 | Table |
| Mart - Aggregate | AGG_O2C_BY_CUSTOMER | ~10 | Table |
| Mart - Aggregate | AGG_O2C_BY_PERIOD | ~3 | Table |

---

For complete details, see `O2C_README.md`.

