# O2C Data Flow & Lineage Guide

**Project:** Honeywell O2C Analytics Platform  
**Date:** December 2, 2025  
**Status:** ✅ Production Ready

---

## 📊 Overview

Complete data lineage from **6 source tables** through **2 transformation layers** to produce **9 business-ready O2C analytics models**.

### Quick Summary

| Aspect | Details |
|--------|---------|
| **Source Tables** | 6 tables (3 facts + 3 dimensions) |
| **Transformation Layers** | 2 layers (Staging with joins → Marts) |
| **Staging Models** | 3 models (each with 1 LEFT JOIN) |
| **Mart Models** | 9 models (3 dimensions + 3 core + 3 aggregates) |
| **dbt Projects** | 2 projects (dbt_o2c + dbt_o2c_semantic) |
| **Semantic Metrics** | 15+ business metrics |

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

📊 MART LAYER (9 Models) - dbt_o2c
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌───────────────────────────────────────────────────────────┐
│ DM_O2C_RECONCILIATION (TABLE) - Main Mart                 │
│ = STG_ENRICHED_ORDERS + STG_ENRICHED_INVOICES +           │
│   STG_ENRICHED_PAYMENTS                                   │
│ JOINs: order_key, invoice_key                             │
│ Output: Complete O2C view with all metrics                │
└─────────────────────────┬─────────────────────────────────┘
                          │
         ┌────────────────┴────────────────┐
         ▼                                 ▼
┌─────────────────────┐        ┌─────────────────────┐
│ AGG_O2C_BY_         │        │ AGG_O2C_BY_PERIOD   │
│ CUSTOMER            │        │                     │
│ (TABLE)             │        │ (TABLE)             │
└─────────────────────┘        └─────────────────────┘
         │
         ▼

🎯 SEMANTIC LAYER (Metadata Only) - dbt_o2c_semantic
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌───────────────────────────────────────────────────────────┐
│ Semantic Models (YAML)                                    │
│ - sm_o2c_reconciliation                                   │
│ - 15+ Metrics defined                                     │
│ - No database objects created                             │
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

## 📈 Data Volume

| Layer | Model | Rows |
|-------|-------|------|
| Source | FACT_SALES_ORDERS | 100 |
| Source | FACT_INVOICES | 80 |
| Source | FACT_PAYMENTS | 60 |
| Staging | STG_ENRICHED_ORDERS | 100 |
| Staging | STG_ENRICHED_INVOICES | 80 |
| Staging | STG_ENRICHED_PAYMENTS | 60 |
| Mart | DM_O2C_RECONCILIATION | 100 |

---

For complete details, see `O2C_README.md`.

