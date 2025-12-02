# Order-to-Cash (O2C) Analytics Platform

**Project:** Honeywell O2C Analytics Platform  
**Date:** December 2, 2025  
**Status:** ✅ Production Ready  
**Version:** 1.0.0

---

## 📊 Overview

This is a **complete, standalone Order-to-Cash (O2C) analytics platform** built with dbt, designed to provide comprehensive insights into the entire order-to-cash cycle from order creation through invoice generation to payment collection.

### **Platform Highlights**

- **🔄 Complete O2C Cycle Tracking**: Order → Invoice → Payment
- **📈 Advanced Analytics**: Cycle time analysis, DSO tracking, collection metrics
- **🎯 Multi-Level Joins**: Joins in staging AND marts for comprehensive enrichment
- **📊 Semantic Layer**: Business-friendly metrics via dbt Semantic Layer
- **✅ 100% Test Coverage**: Comprehensive data quality testing
- **🚀 Production Ready**: Fully documented, tested, and deployable

---

## 🏗️ Architecture

### **Single-Project Structure** (Snowflake Native Compatible)

```
O2C/
├── dbt_o2c/                           # Data transformation with dbt
│   ├── Staging Layer                  # Enriched staging with dimension joins
│   ├── Marts Layer                    # Dimensions, facts, aggregates
│   └── Output: 8 tables/views in Snowflake
│
└── O2C_DEPLOY_SEMANTIC_VIEWS.sql      # Semantic views deployment (manual)
    └── Output: 2 semantic views for Cortex Analyst
```

### **Data Flow**

```
SOURCE TABLES (6 tables)
    ↓
STAGING (3 enriched models with JOINS)
    ├─ Orders + Customer
    ├─ Invoices + Payment Terms
    └─ Payments + Bank Account
    ↓
MARTS (5 models)
    ├─ Dimension (1): Customer
    ├─ Core Marts (2): Reconciliation, Cycle Analysis
    └─ Aggregates (2): By Customer, By Period
    ↓
SEMANTIC VIEWS (deployed separately via SQL)
    ├─ O2C_RECONCILIATION_SEMANTIC (Cortex Analyst)
    └─ O2C_CUSTOMER_METRICS_SEMANTIC (Cortex Analyst)
```

---

## 🚀 Quick Start

### **Prerequisites**

- Snowflake account with appropriate permissions
- dbt CLI or dbt Cloud
- Git

### **Setup (30 minutes)**

```bash
# 1. Load sample data
cd O2C
snowsql -f O2C_LOAD_SAMPLE_DATA.sql

# 2. Build data platform
cd dbt_o2c
dbt deps
dbt build
# ✅ Builds 8 models (3 views + 5 tables)

# 3. Deploy semantic views for Cortex Analyst
cd ..
snowsql -f O2C_DEPLOY_SEMANTIC_VIEWS.sql
# ✅ Creates 2 semantic views
```

**See `O2C_QUICKSTART.md` for detailed step-by-step instructions.**

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| **O2C_README.md** | This file - project overview |
| **O2C_QUICKSTART.md** | Get started in 30 minutes |
| **O2C_SETUP_GUIDE.md** | Detailed setup instructions |
| **O2C_DATA_FLOW_LINEAGE.md** | Complete data lineage documentation |
| **O2C_MONITORING_QUERIES.md** | Health monitoring queries |
| **O2C_DASHBOARD_QUERIES.md** | Business intelligence queries |
| **O2C_LOAD_SAMPLE_DATA.sql** | Sample data loading script |

---

## 📊 Key Metrics Available

### **Volume Metrics**
- Order Count, Invoice Count, Payment Count
- Order Value, Invoice Value, Cash Collected

### **Performance Metrics**
- Days Sales Outstanding (DSO)
- Days to Invoice, Days to Payment
- Order-to-Cash Cycle Time

### **Quality Metrics**
- Collection Rate, Billing Rate
- On-Time Payment Rate
- Reconciliation Status

---

## 🎯 Use Cases

1. **Cash Flow Management**: Track outstanding AR and predict collections
2. **Performance Monitoring**: Measure DSO and cycle times
3. **Customer Analytics**: Identify payment patterns by customer
4. **Process Optimization**: Find bottlenecks in O2C process
5. **Executive Reporting**: KPI dashboards for leadership

---

## 🔧 Project Structure

### **dbt_o2c (Data Platform)**

```
dbt_o2c/
├── models/
│   ├── sources/            # Source definitions
│   ├── staging/            # Enriched staging (with joins)
│   ├── marts/
│   │   ├── dimensions/     # Customer, Payment Terms, Bank
│   │   ├── core/           # Reconciliation, Cycle Analysis
│   │   └── aggregates/     # Pre-aggregated summaries
│   └── metrics/            # dbt metrics (optional)
├── macros/                 # Reusable SQL logic
├── tests/                  # Custom data quality tests
└── analyses/               # Ad-hoc analysis queries
```

### **dbt_o2c_semantic (Semantic Layer)**

```
dbt_o2c_semantic/
└── models/
    └── semantic/
        ├── semantic_models/    # Entity/dimension/measure definitions
        └── metrics/            # Business metric definitions
```

---

## 📈 Sample Queries

### **Get DSO by Customer**

```sql
SELECT
    customer_name,
    customer_type,
    AVG(days_order_to_cash) as avg_dso,
    SUM(outstanding_amount) as total_ar_outstanding
FROM edw_o2c.o2c_core.dm_o2c_reconciliation
WHERE reconciliation_status != 'CLOSED'
GROUP BY 1, 2
ORDER BY total_ar_outstanding DESC;
```

### **Via Semantic Layer API**

```bash
dbt sl query \
  --metrics mtc_days_sales_outstanding \
  --group-by customer_type,metric_time__month
```

---

## 🔍 Monitoring

**Health Check Query:**

```sql
SELECT 
    'Orders' as layer,
    COUNT(*) as row_count,
    MAX(_dbt_loaded_at) as last_refresh
FROM edw_o2c.o2c_staging.stg_enriched_orders

UNION ALL

SELECT 
    'Reconciliation' as layer,
    COUNT(*) as row_count,
    MAX(loaded_at) as last_refresh
FROM edw_o2c.o2c_core.dm_o2c_reconciliation;
```

**See `O2C_MONITORING_QUERIES.md` for comprehensive monitoring.**

---

## 🧪 Testing

```bash
# Run all tests
cd dbt_o2c
dbt test

# Test specific models
dbt test --select staging
dbt test --select marts

# Expected results: 30+ tests passing
```

---

## 🚢 Deployment

### **Development**

```bash
dbt build --target dev
```

### **Production**

```bash
dbt build --target prod
```

### **CI/CD Integration**

See GitHub Actions workflow in `.github/workflows/o2c_deploy.yml`

---

## 👥 Team & Ownership

- **Owner**: O2C Analytics Team
- **Maintained By**: Data Engineering Team
- **Business Sponsor**: Finance Department

---

## 📊 Data Sources

| Source | Table | Purpose |
|--------|-------|---------|
| Orders | `FACT_SALES_ORDERS` | Sales orders from ERP |
| Invoices | `FACT_INVOICES` | Customer invoices |
| Payments | `FACT_PAYMENTS` | Cash receipts |
| Customer | `DIM_CUSTOMER` | Customer master |
| Payment Terms | `DIM_PAYMENT_TERMS` | Payment terms master |
| Bank Accounts | `DIM_BANK_ACCOUNT` | Bank account master |

---

## 🎓 Learning Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-semantic-layer)
- [Snowflake Best Practices](https://docs.snowflake.com/en/user-guide/best-practices.html)

---

## 🆘 Support

For questions or issues:
1. Check documentation in this folder
2. Review `O2C_TROUBLESHOOTING.md`
3. Contact O2C Analytics Team

---

## 📝 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-12-02 | Initial production release |

---

**Last Updated:** December 2, 2025  
**Next Review:** March 2026

