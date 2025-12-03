# O2C Platform Monitoring Queries

**Purpose:** Health monitoring, data quality checks, and operational queries for O2C platform  
**Updated:** December 3, 2025  
**Prerequisite:** Run `O2C_MONITORING_SETUP.sql` first

---

## 📋 Table of Contents

1. [Quick Health Checks](#-quick-health-checks)
2. [Data Freshness](#-data-freshness)
3. [Data Quality Validation](#-data-quality-validation)
4. [Performance Monitoring](#-performance-monitoring)
5. [Business Metrics](#-business-metrics)
6. [Troubleshooting](#-troubleshooting)

---

## 🔍 Quick Health Checks

### **1. Overall Platform Health**

```sql
-- Quick health check (run first!)
SELECT * FROM EDW.O2C_MONITORING.O2C_ALERT_SUMMARY;
```

**Expected Output:**
- `health_status` = '✅ HEALTHY'
- `health_score` >= 90
- `total_critical_alerts` = 0

---

### **2. Active Alerts Summary**

```sql
-- What needs attention right now?
SELECT 
    alert_type,
    severity,
    subject,
    description,
    alert_time
FROM (
    SELECT 'PERFORMANCE' as alert_type, severity, model_name as subject,
           'Model running ' || percent_slower || '% slower' as description, alert_time
    FROM EDW.O2C_MONITORING.O2C_ALERT_PERFORMANCE
    WHERE severity IN ('CRITICAL', 'HIGH')
    
    UNION ALL
    
    SELECT 'MODEL_FAILURE', severity, model_name,
           'Failed ' || failures_last_7_days || 'x in 7 days', failure_time
    FROM EDW.O2C_MONITORING.O2C_ALERT_MODEL_FAILURES
    WHERE severity IN ('CRITICAL', 'HIGH')
    
    UNION ALL
    
    SELECT 'STALE_SOURCE', severity, source_table,
           alert_description, checked_at
    FROM EDW.O2C_MONITORING.O2C_ALERT_STALE_SOURCES
    WHERE severity IN ('CRITICAL', 'HIGH')
)
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    alert_time DESC;
```

---

### **3. Business KPIs Snapshot**

```sql
-- Current business metrics
SELECT * FROM EDW.O2C_MONITORING.O2C_BUSINESS_KPIS;
```

---

## ⏱️ Data Freshness

### **4. Source Table Freshness**

```sql
-- Check when source tables were last loaded
SELECT 
    source_table,
    source_type,
    row_count,
    last_load_timestamp,
    hours_since_load,
    days_since_load,
    freshness_status
FROM EDW.O2C_MONITORING.O2C_SOURCE_FRESHNESS
ORDER BY hours_since_load DESC;
```

---

### **5. Model Layer Freshness**

```sql
-- Check when dbt models were last refreshed
SELECT 
    model_name,
    layer,
    row_count,
    last_refresh,
    minutes_since_refresh,
    freshness_status
FROM EDW.O2C_MONITORING.O2C_MODEL_FRESHNESS
ORDER BY minutes_since_refresh DESC;
```

---

### **6. End-to-End Data Latency**

```sql
-- How fresh is the data in the mart?
SELECT
    'Source → Mart Latency' as metric,
    DATEDIFF('minute', 
        (SELECT MAX(LOAD_TS) FROM EDW.CORP_TRAN.FACT_SALES_ORDERS),
        (SELECT MAX(loaded_at) FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION)
    ) as latency_minutes,
    (SELECT MAX(LOAD_TS) FROM EDW.CORP_TRAN.FACT_SALES_ORDERS) as source_last_load,
    (SELECT MAX(loaded_at) FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION) as mart_last_refresh;
```

---

## ✅ Data Quality Validation

### **7. Row Count Validation**

```sql
-- Validate data flows correctly through layers
SELECT
    'Source: Orders' as checkpoint,
    COUNT(*) as row_count
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS

UNION ALL SELECT 'Source: Invoices', COUNT(*)
FROM EDW.CORP_TRAN.FACT_INVOICES

UNION ALL SELECT 'Source: Payments', COUNT(*)
FROM EDW.CORP_TRAN.FACT_PAYMENTS

UNION ALL SELECT 'Staging: Enriched Orders', COUNT(*)
FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS

UNION ALL SELECT 'Staging: Enriched Invoices', COUNT(*)
FROM EDW.O2C_STAGING.STG_ENRICHED_INVOICES

UNION ALL SELECT 'Staging: Enriched Payments', COUNT(*)
FROM EDW.O2C_STAGING.STG_ENRICHED_PAYMENTS

UNION ALL SELECT 'Mart: Reconciliation', COUNT(*)
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

UNION ALL SELECT 'Mart: Customer Dim', COUNT(*)
FROM EDW.O2C_DIMENSIONS.DIM_O2C_CUSTOMER

UNION ALL SELECT 'Agg: By Customer', COUNT(*)
FROM EDW.O2C_AGGREGATES.AGG_O2C_BY_CUSTOMER

UNION ALL SELECT 'Agg: By Period', COUNT(*)
FROM EDW.O2C_AGGREGATES.AGG_O2C_BY_PERIOD

ORDER BY checkpoint;
```

---

### **8. Join Quality Check**

```sql
-- Check for missing enrichments
SELECT
    'Orders Missing Customer Data' as check_type,
    source_system,
    COUNT(*) as missing_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS), 2) as pct_of_total
FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS
WHERE customer_name IS NULL
GROUP BY source_system

UNION ALL

SELECT
    'Invoices Missing Payment Terms',
    source_system,
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM EDW.O2C_STAGING.STG_ENRICHED_INVOICES), 2)
FROM EDW.O2C_STAGING.STG_ENRICHED_INVOICES
WHERE payment_terms_description IS NULL
GROUP BY source_system

UNION ALL

SELECT
    'Payments Missing Bank Info',
    source_system,
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM EDW.O2C_STAGING.STG_ENRICHED_PAYMENTS), 2)
FROM EDW.O2C_STAGING.STG_ENRICHED_PAYMENTS
WHERE bank_account_id IS NULL
GROUP BY source_system

ORDER BY missing_count DESC;
```

---

### **9. Reconciliation Status Distribution**

```sql
-- How is data distributed across reconciliation statuses?
SELECT
    reconciliation_status,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
    SUM(order_amount) as total_order_value,
    SUM(outstanding_amount) as total_outstanding
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
GROUP BY reconciliation_status
ORDER BY record_count DESC;
```

---

### **10. Data Completeness Check**

```sql
-- Check for null values in critical columns
SELECT
    'order_key' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN order_key IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(SUM(CASE WHEN order_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_pct
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

UNION ALL SELECT 'customer_id', COUNT(*),
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

UNION ALL SELECT 'order_amount', COUNT(*),
    SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

UNION ALL SELECT 'invoice_amount', COUNT(*),
    SUM(CASE WHEN invoice_amount IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN invoice_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

UNION ALL SELECT 'payment_amount', COUNT(*),
    SUM(CASE WHEN payment_amount IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN payment_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

ORDER BY null_pct DESC;
```

---

## ⚡ Performance Monitoring

### **11. Daily Execution Summary**

```sql
-- How did O2C builds perform over time?
SELECT * FROM EDW.O2C_MONITORING.O2C_DAILY_EXECUTION_SUMMARY;
```

---

### **12. Model Performance Ranking**

```sql
-- Which models are slowest?
SELECT * FROM EDW.O2C_MONITORING.O2C_MODEL_PERFORMANCE_RANKING;
```

---

### **13. Recent Executions**

```sql
-- Last 20 model executions
SELECT 
    run_started_at,
    model_name,
    status,
    ROUND(total_node_runtime, 2) as execution_seconds,
    rows_affected,
    warehouse_name
FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
ORDER BY run_started_at DESC
LIMIT 20;
```

---

### **14. Performance Anomalies**

```sql
-- Models running slower than usual
SELECT * FROM EDW.O2C_MONITORING.O2C_ALERT_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM');
```

---

## 📊 Business Metrics

### **15. O2C Cycle Time Trend**

```sql
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT order_key) as total_orders,
    ROUND(AVG(days_order_to_cash), 1) as avg_dso,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_order_to_cash) as median_dso,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_order_to_cash) as p90_dso
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE days_order_to_cash IS NOT NULL
  AND reconciliation_status = 'CLOSED'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 12;
```

---

### **16. Collection Performance**

```sql
SELECT
    reconciliation_status,
    payment_timing,
    COUNT(*) as count,
    SUM(outstanding_amount) as total_outstanding,
    ROUND(AVG(days_past_due), 1) as avg_days_past_due
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE invoice_id IS NOT NULL
GROUP BY 1, 2
ORDER BY total_outstanding DESC;
```

---

### **17. Customer Summary**

```sql
-- Top customers by order volume
SELECT 
    customer_id,
    customer_name,
    total_orders,
    total_order_value,
    total_paid,
    total_outstanding,
    avg_days_to_cash as avg_dso,
    on_time_payment_rate_pct,
    loaded_at
FROM EDW.O2C_AGGREGATES.AGG_O2C_BY_CUSTOMER
ORDER BY total_order_value DESC
LIMIT 20;
```

---

## 🔧 Troubleshooting

### **18. Model Failures**

```sql
-- Recent model failures
SELECT * FROM EDW.O2C_MONITORING.O2C_ALERT_MODEL_FAILURES
ORDER BY failure_time DESC;
```

---

### **19. Stale Source Alerts**

```sql
-- Sources that need attention
SELECT * FROM EDW.O2C_MONITORING.O2C_ALERT_STALE_SOURCES
ORDER BY hours_since_load DESC;
```

---

### **20. Orphan Records Check**

```sql
-- Orders without invoices (expected for recent orders)
SELECT 
    source_system,
    DATE_TRUNC('month', order_date) as order_month,
    COUNT(*) as orders_without_invoice
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE invoice_key = 'NOT_INVOICED'
GROUP BY 1, 2
ORDER BY 2 DESC, 1;
```

---

### **21. Duplicate Key Check**

```sql
-- Verify no duplicate keys in mart
SELECT 
    order_key,
    invoice_key,
    payment_key,
    COUNT(*) as duplicate_count
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;
```

---

## ✅ Summary

| Category | Queries | Purpose |
|----------|---------|---------|
| Quick Health | 3 | Immediate status check |
| Freshness | 3 | Data timeliness |
| Quality | 4 | Data validation |
| Performance | 4 | Build monitoring |
| Business | 3 | KPI tracking |
| Troubleshooting | 4 | Issue diagnosis |

**Total:** 21 monitoring queries

---

For dashboard visualization, see `O2C_DASHBOARD_QUERIES.md`.
