# O2C Platform Monitoring Queries

**Purpose:** Health monitoring and data quality checks for O2C platform  
**Updated:** December 2, 2025

---

## 🔍 System Health Checks

### **1. Data Freshness Check**

```sql
SELECT 
    'Orders' as layer,
    'Staging' as type,
    COUNT(*) as row_count,
    MAX(_dbt_loaded_at) as last_refresh,
    DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) as minutes_since_refresh
FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS

UNION ALL

SELECT 
    'Reconciliation' as layer,
    'Mart' as type,
    COUNT(*) as row_count,
    MAX(loaded_at) as last_refresh,
    DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) as minutes_since_refresh
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION;
```

### **2. Row Count Validation**

```sql
SELECT
    'Source: Orders' as check_point,
    COUNT(*) as row_count
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS

UNION ALL

SELECT
    'Staging: Enriched Orders' as check_point,
    COUNT(*) as row_count
FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS

UNION ALL

SELECT
    'Mart: Reconciliation' as check_point,
    COUNT(*) as row_count
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION;
```

### **3. Join Quality Check**

```sql
-- Check for orders missing customer data
SELECT
    source_system,
    COUNT(*) as orders_without_customer,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct_missing
FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS
WHERE customer_name IS NULL
GROUP BY source_system;
```

---

## 📊 Business Metrics Monitoring

### **4. O2C Cycle Time Trend**

```sql
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT order_key) as total_orders,
    AVG(days_order_to_cash) as avg_dso,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_order_to_cash) as median_dso
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE days_order_to_cash IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC
LIMIT 12;
```

### **5. Collection Performance**

```sql
SELECT
    reconciliation_status,
    payment_timing,
    COUNT(*) as count,
    SUM(outstanding_amount) as total_outstanding
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
GROUP BY 1, 2
ORDER BY 4 DESC;
```

---

For complete monitoring setup, see `O2C_SETUP_GUIDE.md`.

