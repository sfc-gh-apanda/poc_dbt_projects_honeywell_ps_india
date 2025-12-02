# O2C Dashboard Queries

**Purpose:** Pre-built queries for O2C analytics dashboards  
**Platform:** Snowsight / Tableau / Power BI  
**Updated:** December 2, 2025

---

## 📊 Executive Dashboard

### **1. O2C Summary Metrics (KPI Cards)**

```sql
SELECT
    COUNT(DISTINCT order_key) as total_orders,
    COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) as invoiced_orders,
    COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) as paid_orders,
    
    SUM(order_amount) as total_order_value,
    SUM(outstanding_amount) as total_ar_outstanding,
    
    AVG(days_order_to_cash) as avg_dso,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN payment_timing = 'ON_TIME' THEN payment_key END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END), 0),
        1
    ) as on_time_payment_pct
    
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE order_date >= DATEADD('month', -12, CURRENT_DATE());
```

### **2. Monthly Trend - Order to Cash**

```sql
SELECT
    DATE_TRUNC('month', order_date) as month,
    
    -- Volume
    COUNT(DISTINCT order_key) as orders,
    COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) as invoices,
    COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) as payments,
    
    -- Value
    SUM(order_amount) as order_value,
    SUM(invoice_amount) as invoice_value,
    SUM(payment_amount) as cash_collected,
    
    -- Performance
    AVG(days_order_to_cash) as avg_dso
    
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE order_date >= DATEADD('month', -12, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;
```

---

## 💰 Collections Dashboard

### **3. AR Aging Summary**

```sql
SELECT
    CASE
        WHEN days_past_due <= 0 THEN 'Current'
        WHEN days_past_due BETWEEN 1 AND 30 THEN '1-30 Days'
        WHEN days_past_due BETWEEN 31 AND 60 THEN '31-60 Days'
        WHEN days_past_due BETWEEN 61 AND 90 THEN '61-90 Days'
        ELSE '90+ Days'
    END as aging_category,
    
    COUNT(*) as invoice_count,
    SUM(outstanding_amount) as total_outstanding
    
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE reconciliation_status IN ('NOT_PAID', 'OPEN')
  AND invoice_id IS NOT NULL
GROUP BY 1
ORDER BY 
    CASE aging_category
        WHEN 'Current' THEN 1
        WHEN '1-30 Days' THEN 2
        WHEN '31-60 Days' THEN 3
        WHEN '61-90 Days' THEN 4
        ELSE 5
    END;
```

### **4. Top Customers by Outstanding AR**

```sql
SELECT
    customer_name,
    customer_type,
    customer_country,
    COUNT(DISTINCT order_key) as open_orders,
    SUM(outstanding_amount) as total_ar_outstanding,
    AVG(days_past_due) as avg_days_past_due,
    MAX(days_past_due) as max_days_past_due
    
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE reconciliation_status IN ('NOT_PAID', 'OPEN')
  AND outstanding_amount > 0
GROUP BY 1, 2, 3
ORDER BY total_ar_outstanding DESC
LIMIT 20;
```

---

## 🎯 Performance Dashboard

### **5. Cycle Time Analysis by Customer Type**

```sql
SELECT
    customer_type,
    
    COUNT(*) as completed_transactions,
    
    -- Cycle times
    ROUND(AVG(days_order_to_invoice), 1) as avg_days_to_invoice,
    ROUND(AVG(days_invoice_to_payment), 1) as avg_days_to_payment,
    ROUND(AVG(days_order_to_cash), 1) as avg_days_to_cash,
    
    -- Percentiles
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_order_to_cash) as median_dso,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_order_to_cash) as p90_dso
    
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE reconciliation_status = 'CLOSED'
  AND order_date >= DATEADD('month', -6, CURRENT_DATE())
GROUP BY 1;
```

---

For more queries, see `O2C_README.md`.

