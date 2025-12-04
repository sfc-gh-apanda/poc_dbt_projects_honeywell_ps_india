# O2C Platform Dashboard Queries

**Purpose:** Complete Snowsight dashboard queries for O2C analytics and monitoring  
**Platform:** Snowsight / Tableau / Power BI  
**Updated:** December 3, 2025  
**Prerequisite:** Run `O2C_MONITORING_SETUP.sql` first

---

## 📋 Table of Contents

1. [Executive Dashboard](#-executive-dashboard)
2. [Operations Monitoring](#-operations-monitoring)
3. [Data Quality & Freshness](#-data-quality--freshness)
4. [Performance Monitoring](#-performance-monitoring)
5. [Cost & Resources](#-cost--resources)
6. [Alerts & Health](#-alerts--health)
7. [Dashboard Setup Guide](#-dashboard-setup-guide)

---

## 📊 Executive Dashboard

### **TILE 1: O2C Executive Scorecard**

**Purpose:** Key business metrics at a glance  
**Type:** Scorecard (6 metrics)  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Executive KPI Scorecard
-- ============================================================================
SELECT 
    -- Volume
    total_orders,
    invoiced_orders,
    paid_orders,
    
    -- Values (in thousands)
    ROUND(total_order_value / 1000, 1) as order_value_k,
    ROUND(total_ar_outstanding / 1000, 1) as ar_outstanding_k,
    
    -- Performance
    avg_dso,
    median_dso,
    on_time_payment_pct,
    
    -- Conversion
    billing_rate_pct,
    collection_rate_pct,
    
    -- Health
    health_status
FROM EDW.O2C_MONITORING.O2C_BUSINESS_KPIS
CROSS JOIN EDW.O2C_MONITORING.O2C_ALERT_SUMMARY;
```

---

### **TILE 2: Monthly O2C Trend**

**Purpose:** Track O2C flow over time  
**Type:** Multi-line chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- Monthly Order-to-Cash Trend (Last 12 Months)
-- ============================================================================
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
    ROUND(AVG(days_order_to_cash), 1) as avg_dso
    
FROM EDW.O2C_STAGING_O2C_CORE.DM_O2C_RECONCILIATION
WHERE order_date >= DATEADD('month', -12, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;
```

**Chart Configuration:**
- X-axis: `month`
- Y-axes:
  - Primary: `orders`, `invoices`, `payments`
  - Secondary: `avg_dso`
- Chart type: Line chart with multiple series

---

### **TILE 3: AR Aging Distribution**

**Purpose:** Visual of outstanding receivables  
**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- AR Aging Buckets
-- ============================================================================
SELECT
    CASE
        WHEN days_past_due <= 0 THEN '1. Current'
        WHEN days_past_due BETWEEN 1 AND 30 THEN '2. 1-30 Days'
        WHEN days_past_due BETWEEN 31 AND 60 THEN '3. 31-60 Days'
        WHEN days_past_due BETWEEN 61 AND 90 THEN '4. 61-90 Days'
        ELSE '5. 90+ Days'
    END as aging_bucket,
    
    COUNT(*) as invoice_count,
    SUM(outstanding_amount) as total_outstanding,
    ROUND(AVG(outstanding_amount), 2) as avg_outstanding,
    
    CASE
        WHEN days_past_due <= 0 THEN '🟢'
        WHEN days_past_due BETWEEN 1 AND 30 THEN '🟡'
        WHEN days_past_due BETWEEN 31 AND 60 THEN '🟠'
        WHEN days_past_due BETWEEN 61 AND 90 THEN '🔴'
        ELSE '⚫'
    END as risk_indicator
    
FROM EDW.O2C_STAGING_O2C_CORE.DM_O2C_RECONCILIATION
WHERE reconciliation_status IN ('NOT_PAID', 'OPEN')
  AND invoice_id IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

---

### **TILE 4: Top Customers by AR**

**Purpose:** Focus on highest AR exposure  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- Top 15 Customers by Outstanding AR
-- ============================================================================
SELECT
    customer_name,
    customer_type,
    customer_country,
    COUNT(DISTINCT order_key) as open_orders,
    SUM(outstanding_amount) as total_ar_outstanding,
    ROUND(AVG(days_past_due), 1) as avg_days_past_due,
    MAX(days_past_due) as max_days_past_due,
    CASE 
        WHEN MAX(days_past_due) > 90 THEN '🔴 High Risk'
        WHEN MAX(days_past_due) > 60 THEN '🟠 Medium Risk'
        WHEN MAX(days_past_due) > 30 THEN '🟡 Low Risk'
        ELSE '🟢 Current'
    END as risk_status
    
FROM EDW.O2C_STAGING_O2C_CORE.DM_O2C_RECONCILIATION
WHERE reconciliation_status IN ('NOT_PAID', 'OPEN')
  AND outstanding_amount > 0
GROUP BY 1, 2, 3
ORDER BY total_ar_outstanding DESC
LIMIT 15;
```

---

## 🔧 Operations Monitoring

### **TILE 5: Daily Execution Summary**

**Purpose:** Track dbt build performance  
**Type:** Multi-line chart  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Daily Build Execution Trend (Last 30 Days)
-- ============================================================================
SELECT 
    execution_date,
    models_run,
    successful_models,
    failed_models,
    total_minutes,
    avg_execution_seconds,
    success_rate_pct
FROM EDW.O2C_MONITORING.O2C_DAILY_EXECUTION_SUMMARY
ORDER BY execution_date DESC;
```

**Chart Configuration:**
- X-axis: `execution_date`
- Y-axes:
  - Primary: `models_run`, `total_minutes`
  - Secondary: `success_rate_pct`

---

### **TILE 6: Model Performance Ranking**

**Purpose:** Identify optimization opportunities  
**Type:** Horizontal bar chart  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Model Performance (Slowest Models)
-- ============================================================================
SELECT 
    model_name,
    schema_name,
    run_count,
    avg_seconds,
    max_seconds,
    total_seconds,
    estimated_cost_usd,
    performance_tier
FROM EDW.O2C_MONITORING.O2C_SLOWEST_MODELS;
```

**Chart Configuration:**
- Chart type: Horizontal bar
- X-axis: `avg_seconds`
- Y-axis: `model_name`
- Color by: `performance_tier`

---

## 🔍 Data Quality & Freshness

### **TILE 7: Source Table Freshness**

**Purpose:** Monitor upstream data availability  
**Type:** Table with status indicators  
**Refresh:** Every 30 minutes

```sql
-- ============================================================================
-- O2C Source Table Freshness Status
-- ============================================================================
SELECT 
    source_table,
    source_type,
    row_count,
    last_load_timestamp,
    hours_since_load,
    freshness_status,
    CASE 
        WHEN freshness_status LIKE '%Fresh%' THEN 'GREEN'
        WHEN freshness_status LIKE '%Warning%' THEN 'YELLOW'
        ELSE 'RED'
    END as status_color
FROM EDW.O2C_MONITORING.O2C_SOURCE_FRESHNESS
ORDER BY hours_since_load DESC;
```

**Conditional Formatting:**
- Green: freshness_status contains 'Fresh'
- Yellow: freshness_status contains 'Warning'
- Red: freshness_status contains 'Stale'

---

### **TILE 8: Model Layer Freshness**

**Purpose:** Monitor dbt model freshness  
**Type:** Table with status indicators  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Model Layer Freshness Status
-- ============================================================================
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

### **TILE 9: Data Flow Row Counts**

**Purpose:** Validate data flow through layers  
**Type:** Funnel or table  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Data Flow Validation (Row Counts)
-- ============================================================================
-- Source Layer
SELECT '1. Source: Orders' as checkpoint, COUNT(*) as row_count
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
UNION ALL
SELECT '1. Source: Invoices', COUNT(*)
FROM EDW.CORP_TRAN.FACT_INVOICES
UNION ALL
SELECT '1. Source: Payments', COUNT(*)
FROM EDW.CORP_TRAN.FACT_PAYMENTS

UNION ALL

-- Staging Layer
SELECT '2. Staging: Orders', COUNT(*)
FROM EDW.O2C_STAGING_O2C_STAGING.STG_ENRICHED_ORDERS
UNION ALL
SELECT '2. Staging: Invoices', COUNT(*)
FROM EDW.O2C_STAGING_O2C_STAGING.STG_ENRICHED_INVOICES
UNION ALL
SELECT '2. Staging: Payments', COUNT(*)
FROM EDW.O2C_STAGING_O2C_STAGING.STG_ENRICHED_PAYMENTS

UNION ALL

-- Mart Layer
SELECT '3. Mart: Reconciliation', COUNT(*)
FROM EDW.O2C_STAGING_O2C_CORE.DM_O2C_RECONCILIATION
UNION ALL
SELECT '3. Mart: Customer Agg', COUNT(*)
FROM EDW.O2C_STAGING_O2C_AGGREGATES.AGG_O2C_BY_CUSTOMER
UNION ALL
SELECT '3. Mart: Period Agg', COUNT(*)
FROM EDW.O2C_STAGING_O2C_AGGREGATES.AGG_O2C_BY_PERIOD

ORDER BY checkpoint;
```

---

### **TILE 10: Join Quality Check**

**Purpose:** Monitor data enrichment quality  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Join Quality - Missing Enrichments
-- ============================================================================
SELECT
    'Orders Missing Customer' as check_type,
    source_system,
    COUNT(*) as missing_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct_missing
FROM EDW.O2C_STAGING_O2C_STAGING.STG_ENRICHED_ORDERS
WHERE customer_name IS NULL
GROUP BY source_system

UNION ALL

SELECT
    'Invoices Missing Payment Terms',
    source_system,
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)
FROM EDW.O2C_STAGING_O2C_STAGING.STG_ENRICHED_INVOICES
WHERE payment_terms_description IS NULL
GROUP BY source_system

ORDER BY missing_count DESC;
```

---

## ⚡ Performance Monitoring

### **TILE 11: Model Execution History**

**Purpose:** Detailed execution log  
**Type:** Table  
**Refresh:** Real-time

```sql
-- ============================================================================
-- Recent O2C Model Executions (Last 24 Hours)
-- ============================================================================
SELECT 
    run_started_at,
    model_name,
    schema_name,
    status,
    ROUND(total_node_runtime, 2) as execution_seconds,
    rows_affected,
    warehouse_name,
    user_name
FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY run_started_at DESC
LIMIT 50;
```

---

### **TILE 12: Performance Anomaly Detection**

**Purpose:** Identify models running slower than baseline  
**Type:** Table with severity  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Performance Anomalies
-- ============================================================================
SELECT 
    model_name,
    baseline_seconds,
    recent_avg_seconds,
    recent_max_seconds,
    seconds_slower,
    percent_slower,
    severity,
    recent_run_count
FROM EDW.O2C_MONITORING.O2C_ALERT_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    percent_slower DESC;
```

---

## 💰 Cost & Resources

### **TILE 13: O2C Query Costs (Estimated)**

**Purpose:** Track compute costs  
**Type:** Area chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Daily Query Cost Estimate (Last 30 Days)
-- ============================================================================
WITH daily_queries AS (
    SELECT 
        DATE(run_started_at) as query_date,
        COUNT(*) as query_count,
        ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes,
        -- Estimated cost: assumes X-Small warehouse at $2/hour
        ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) as estimated_cost_usd
    FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
      AND status = 'SUCCESS'
    GROUP BY 1
)
SELECT 
    query_date,
    query_count,
    total_minutes,
    estimated_cost_usd,
    SUM(estimated_cost_usd) OVER (ORDER BY query_date) as cumulative_cost_usd
FROM daily_queries
ORDER BY query_date DESC;
```

---

### **TILE 14: Cost by Model**

**Purpose:** Identify expensive models  
**Type:** Horizontal bar  
**Refresh:** Weekly

```sql
-- ============================================================================
-- O2C Cost by Model (Last 7 Days)
-- ============================================================================
SELECT 
    model_name,
    run_count,
    avg_seconds,
    total_seconds,
    estimated_cost_usd,
    performance_tier
FROM EDW.O2C_MONITORING.O2C_SLOWEST_MODELS
ORDER BY estimated_cost_usd DESC;
```

---

## 🚨 Alerts & Health

### **TILE 15: O2C Health Summary**

**Purpose:** Overall platform health  
**Type:** Scorecard  
**Refresh:** Every 5 minutes

```sql
-- ============================================================================
-- O2C Platform Health Summary
-- ============================================================================
SELECT 
    health_status,
    health_score,
    
    -- Alert counts
    critical_performance_issues,
    high_performance_issues,
    critical_model_failures,
    high_model_failures,
    critical_stale_sources,
    high_stale_sources,
    total_critical_alerts,
    
    snapshot_time
FROM EDW.O2C_MONITORING.O2C_ALERT_SUMMARY;
```

**Display:**
- Health Score as gauge (0-100)
- Green: 90-100
- Yellow: 70-89
- Red: <70

---

### **TILE 16: Active Alerts**

**Purpose:** Current alerts requiring attention  
**Type:** Table  
**Refresh:** Every 5 minutes

```sql
-- ============================================================================
-- O2C Active Alerts
-- ============================================================================
-- Performance Alerts
SELECT 
    'PERFORMANCE' as alert_type,
    severity,
    model_name as subject,
    'Model running ' || percent_slower || '% slower than baseline' as description,
    alert_time
FROM EDW.O2C_MONITORING.O2C_ALERT_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

-- Model Failure Alerts
SELECT 
    'MODEL_FAILURE',
    severity,
    model_name,
    'Model failed ' || failures_last_7_days || ' times in last 7 days',
    failure_time
FROM EDW.O2C_MONITORING.O2C_ALERT_MODEL_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

-- Stale Source Alerts
SELECT 
    'STALE_SOURCE',
    severity,
    source_table,
    alert_description,
    checked_at
FROM EDW.O2C_MONITORING.O2C_ALERT_STALE_SOURCES
WHERE severity IN ('CRITICAL', 'HIGH')

ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    alert_time DESC;
```

---

### **TILE 17: Model Failures Log**

**Purpose:** Track recent failures  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Model Failures (Last 24 Hours)
-- ============================================================================
SELECT 
    failure_time,
    model_name,
    warehouse_name,
    execution_seconds,
    failures_last_7_days,
    severity
FROM EDW.O2C_MONITORING.O2C_ALERT_MODEL_FAILURES
ORDER BY failure_time DESC;
```

---

## 📈 Advanced Operations Tiles

### **TILE 18: dbt Operations Scorecard**

**Purpose:** Key dbt metrics at a glance (Models, Tests, Execution, Cost)  
**Type:** Scorecard (4 metrics)  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C dbt Operations Scorecard
-- ============================================================================
WITH today_runs AS (
    SELECT 
        COUNT(DISTINCT model_name) as models_run,
        SUM(total_node_runtime) as total_seconds,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_models,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_models
    FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
    WHERE DATE(run_started_at) = CURRENT_DATE()
),
today_tests AS (
    SELECT 
        COUNT(*) as total_tests,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as passed_tests,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_tests
    FROM EDW.O2C_MONITORING.O2C_TEST_EXECUTIONS
    WHERE DATE(run_started_at) = CURRENT_DATE()
)
SELECT 
    r.models_run,
    r.successful_models,
    r.failed_models,
    ROUND(r.total_seconds / 60, 1) as total_minutes,
    t.total_tests,
    t.passed_tests,
    t.failed_tests,
    ROUND(t.passed_tests * 100.0 / NULLIF(t.total_tests, 0), 1) as test_pass_rate_pct,
    -- Estimated cost (assumes $2/hour warehouse)
    ROUND((r.total_seconds / 3600) * 2.0, 2) as daily_cost_usd
FROM today_runs r
CROSS JOIN today_tests t;
```

---

### **TILE 19: Test Health Trend**

**Purpose:** Monitor data quality test pass rates over time  
**Type:** Stacked area chart  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Test Pass Rate Trend (Last 30 Days)
-- ============================================================================
SELECT 
    DATE(run_started_at) as test_date,
    status,
    COUNT(*) as test_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(run_started_at)), 2) as percentage
FROM EDW.O2C_MONITORING.O2C_TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY test_date, status
ORDER BY test_date DESC, status;
```

**Chart Configuration:**
- Chart type: Stacked area chart
- X-axis: `test_date`
- Y-axis: `test_count`
- Series: Group by `status`
- Colors: pass=Green, fail=Red, warn=Yellow

---

### **TILE 20: Failed Tests Alert**

**Purpose:** Real-time alert for test failures  
**Type:** Table with conditional formatting  
**Refresh:** Every 5 minutes

```sql
-- ============================================================================
-- O2C Recent Test Failures (Last 24 Hours)
-- ============================================================================
SELECT 
    run_started_at as failure_time,
    test_type,
    node_id as test_name,
    status,
    rows_produced as failed_row_count,
    ROUND(total_node_runtime, 2) as execution_seconds
FROM EDW.O2C_MONITORING.O2C_TEST_EXECUTIONS
WHERE status = 'FAIL'
  AND run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY run_started_at DESC;
```

**Alert Configuration:**
- Send email when: `row_count > 0`
- Recipients: data-team@company.com
- Subject: "🚨 O2C Test Failures Detected"

---

### **TILE 21: Model Execution Time Distribution**

**Purpose:** Understand execution time patterns  
**Type:** Histogram / Bar chart  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Model Execution Time Distribution (Last 7 Days)
-- ============================================================================
WITH execution_buckets AS (
    SELECT 
        total_node_runtime,
        CASE 
            WHEN total_node_runtime < 1 THEN '< 1s'
            WHEN total_node_runtime < 5 THEN '1-5s'
            WHEN total_node_runtime < 10 THEN '5-10s'
            WHEN total_node_runtime < 30 THEN '10-30s'
            WHEN total_node_runtime < 60 THEN '30-60s'
            WHEN total_node_runtime < 300 THEN '1-5min'
            ELSE '> 5min'
        END as time_bucket,
        CASE 
            WHEN total_node_runtime < 1 THEN 1
            WHEN total_node_runtime < 5 THEN 2
            WHEN total_node_runtime < 10 THEN 3
            WHEN total_node_runtime < 30 THEN 4
            WHEN total_node_runtime < 60 THEN 5
            WHEN total_node_runtime < 300 THEN 6
            ELSE 7
        END as bucket_order
    FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
      AND status = 'SUCCESS'
)
SELECT 
    time_bucket,
    COUNT(*) as model_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM execution_buckets
GROUP BY time_bucket, bucket_order
ORDER BY bucket_order;
```

---

### **TILE 22: Warehouse Utilization**

**Purpose:** Track warehouse usage by O2C jobs  
**Type:** Stacked bar chart  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Warehouse Usage (Last 7 Days)
-- ============================================================================
SELECT 
    DATE(run_started_at) as usage_date,
    warehouse_name,
    COUNT(*) as query_count,
    ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes,
    -- Estimated credits (approximate)
    ROUND(SUM(total_node_runtime) / 3600 * 1.0, 4) as estimated_credits
FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY usage_date, warehouse_name
ORDER BY usage_date DESC, total_minutes DESC;
```

**Chart Configuration:**
- Chart type: Stacked bar
- X-axis: `usage_date`
- Y-axis: `total_minutes`
- Stack by: `warehouse_name`

---

### **TILE 23: Test Coverage by Model**

**Purpose:** Monitor test coverage and health per model  
**Type:** Table with health scores  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Test Coverage Heatmap (Last 7 Days)
-- ============================================================================
WITH model_tests AS (
    SELECT 
        test_type,
        COUNT(*) as test_count,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as passed,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed
    FROM EDW.O2C_MONITORING.O2C_TEST_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY test_type
)
SELECT 
    test_type,
    test_count,
    passed,
    failed,
    ROUND(passed * 100.0 / NULLIF(test_count, 0), 1) as pass_rate_pct,
    CASE 
        WHEN passed * 100.0 / NULLIF(test_count, 0) = 100 THEN '✅ Excellent'
        WHEN passed * 100.0 / NULLIF(test_count, 0) >= 95 THEN '✅ Good'
        WHEN passed * 100.0 / NULLIF(test_count, 0) >= 90 THEN '⚠️ Fair'
        ELSE '❌ Poor'
    END as health_status
FROM model_tests
WHERE test_count > 0
ORDER BY pass_rate_pct ASC;
```

---

### **TILE 24: Optimization Priorities**

**Purpose:** Identify high-cost, low-efficiency models  
**Type:** Prioritized table  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Models to Optimize (Slowest + Most Resource Intensive)
-- ============================================================================
SELECT 
    model_name,
    schema_name,
    run_count,
    avg_seconds,
    max_seconds,
    total_seconds,
    estimated_cost_usd,
    performance_tier,
    CASE 
        WHEN total_seconds > 3600 THEN '🔴 High Priority'
        WHEN total_seconds > 600 THEN '🟡 Medium Priority'
        ELSE '🟢 Low Priority'
    END as optimization_priority
FROM EDW.O2C_MONITORING.O2C_SLOWEST_MODELS
WHERE total_seconds > 10  -- Focus on models taking >10 seconds total
ORDER BY total_seconds DESC;
```

---

### **TILE 25: Week-over-Week Comparison**

**Purpose:** Track key metrics week-over-week  
**Type:** Comparison table  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Week-over-Week Performance Comparison
-- ============================================================================
WITH this_week AS (
    SELECT 
        COUNT(DISTINCT model_name) as models,
        ROUND(SUM(total_node_runtime) / 60, 1) as total_minutes,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failures,
        ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) as estimated_cost_usd
    FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
    WHERE run_started_at >= DATE_TRUNC('week', CURRENT_DATE())
),
last_week AS (
    SELECT 
        COUNT(DISTINCT model_name) as models,
        ROUND(SUM(total_node_runtime) / 60, 1) as total_minutes,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failures,
        ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) as estimated_cost_usd
    FROM EDW.O2C_MONITORING.O2C_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(week, -1, DATE_TRUNC('week', CURRENT_DATE()))
      AND run_started_at < DATE_TRUNC('week', CURRENT_DATE())
)
SELECT 
    'This Week' as period,
    tw.models,
    tw.total_minutes,
    tw.failures,
    tw.estimated_cost_usd
FROM this_week tw

UNION ALL

SELECT 
    'Last Week',
    lw.models,
    lw.total_minutes,
    lw.failures,
    lw.estimated_cost_usd
FROM last_week lw

UNION ALL

SELECT 
    'Change (%)',
    ROUND((tw.models - lw.models) * 100.0 / NULLIF(lw.models, 0), 1),
    ROUND((tw.total_minutes - lw.total_minutes) * 100.0 / NULLIF(lw.total_minutes, 0), 1),
    ROUND((tw.failures - lw.failures) * 100.0 / NULLIF(lw.failures, 0), 1),
    ROUND((tw.estimated_cost_usd - lw.estimated_cost_usd) * 100.0 / NULLIF(lw.estimated_cost_usd, 0), 1)
FROM this_week tw, last_week lw;
```

**Display:**
- 3-row comparison table
- Color-code positive/negative changes

---

### **TILE 26: Error Rate Trend**

**Purpose:** Track error rates over time  
**Type:** Line chart with threshold  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Error Rate Trend (Last 30 Days)
-- ============================================================================
SELECT 
    date,
    total_queries,
    error_count,
    success_count,
    error_rate_pct,
    success_rate_pct,
    error_rate_7day_avg
FROM EDW.O2C_MONITORING.O2C_ERROR_TREND
ORDER BY date DESC;
```

**Chart Configuration:**
- Line 1: `error_rate_pct` (solid red line)
- Line 2: `error_rate_7day_avg` (dashed orange line)
- Threshold line at 5% (alert level)

---

## 🎨 Dashboard Setup Guide

### **Step 1: Run Prerequisites**

```sql
-- 1. First, run the O2C monitoring setup
@O2C_MONITORING_SETUP.sql

-- 2. Verify views exist
SELECT * FROM EDW.O2C_MONITORING.O2C_ALERT_SUMMARY;
SELECT * FROM EDW.O2C_MONITORING.O2C_SOURCE_FRESHNESS;
```

### **Step 2: Create Snowsight Dashboard**

1. Log into Snowsight
2. Click **Dashboards** in left navigation
3. Click **+ Dashboard**
4. Name: "O2C Analytics Platform"
5. Description: "Order-to-Cash monitoring and analytics"

### **Step 3: Add Tiles**

For each query above:
1. Click **+ Add Tile**
2. Select **From SQL Query**
3. Paste the SQL query
4. Click **Run**
5. Configure visualization
6. Set refresh schedule
7. Save tile

### **Step 4: Recommended Layout**

```
┌─────────────────────────────────────────────────────────────┐
│  O2C ANALYTICS PLATFORM                                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  [TILE 15: Health] [TILE 1: Executive Scorecard - 6 KPIs]  │
│                                                              │
├──────────────────────────┬──────────────────────────────────┤
│  TILE 2: Monthly Trend   │  TILE 3: AR Aging                │
│  (Line Chart)            │  (Bar Chart)                     │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 4: Top Customers   │  TILE 16: Active Alerts          │
│  (Table)                 │  (Table)                         │
│                          │                                   │
├──────────────────────────┴──────────────────────────────────┤
│                                                              │
│  [TILE 7: Source Freshness] [TILE 8: Model Freshness]      │
│                                                              │
├──────────────────────────┬──────────────────────────────────┤
│  TILE 5: Execution Trend │  TILE 6: Slowest Models          │
│  (Line Chart)            │  (Bar Chart)                     │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 13: Cost Trend     │  TILE 14: Cost by Model          │
│  (Area Chart)            │  (Bar Chart)                     │
│                          │                                   │
└──────────────────────────┴──────────────────────────────────┘
```

### **Step 5: Configure Alerts**

| Tile | Condition | Frequency | Recipients |
|------|-----------|-----------|------------|
| TILE 15 | health_score < 70 | Immediate | o2c-team@company.com |
| TILE 16 | row_count > 0 | Every 15 min | data-team@company.com |
| TILE 17 | severity = 'CRITICAL' | Immediate | on-call@company.com |
| TILE 7 | Any '❌ Stale' | Every 30 min | data-ops@company.com |

### **Step 6: Share Dashboard**

1. Click **Share** button
2. Add users/roles
3. Set permissions (View / Edit)
4. Copy dashboard link

---

## ✅ Summary

**Total Tiles:** 26 dashboard queries

**Coverage:**
| Category | Tiles | Description |
|----------|-------|-------------|
| Executive KPIs | 4 tiles | Business scorecard, trends, AR aging, customers |
| Operations Monitoring | 2 tiles | Execution trend, model performance |
| Data Quality | 4 tiles | Source freshness, model freshness, row counts, joins |
| Performance | 2 tiles | Execution history, anomaly detection |
| Cost Tracking | 2 tiles | Cost trend, cost by model |
| Alerts & Health | 3 tiles | Health summary, active alerts, failure log |
| **Advanced Operations** | **9 tiles** | **dbt scorecard, test health, failed tests, time distribution, warehouse usage, test coverage, optimization, weekly comparison, error trend** |

### Complete Dashboard Layout:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  O2C ANALYTICS PLATFORM - COMPLETE DASHBOARD                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  [TILE 18: dbt Scorecard - Models|Tests|Time|Cost]                      │
│                                                                          │
├─────────────────────────────┬───────────────────────────────────────────┤
│  TILE 5: Execution Trend    │  TILE 6: Slowest Models                   │
│  (Line Chart - 30 days)     │  (Bar Chart - Top 10)                     │
├─────────────────────────────┼───────────────────────────────────────────┤
│  TILE 19: Test Health       │  TILE 13: Cost Trend                      │
│  (Stacked Area)             │  (Area Chart - 30 days)                   │
├─────────────────────────────┼───────────────────────────────────────────┤
│  TILE 14: Expensive Models  │  TILE 20: Failed Tests ⚠️                 │
│  (Bar Chart - Top 10)       │  (Alert Table - Real-time)                │
├─────────────────────────────┼───────────────────────────────────────────┤
│  TILE 21: Time Distribution │  TILE 22: Warehouse Usage                 │
│  (Histogram)                │  (Stacked Bar)                            │
├─────────────────────────────┼───────────────────────────────────────────┤
│  TILE 7: Source Freshness   │  TILE 12: Performance Anomalies           │
│  (Table with status)        │  (Alert Table)                            │
├─────────────────────────────┼───────────────────────────────────────────┤
│  TILE 23: Test Coverage     │  TILE 24: Optimization Ideas              │
│  (Table - Health Score)     │  (Priority Table)                         │
├─────────────────────────────┴───────────────────────────────────────────┤
│  TILE 25: Weekly Summary (3-row Comparison Table)                       │
├─────────────────────────────────────────────────────────────────────────┤
│  TILE 26: Error Rate Trend (Line Chart with 7-day Moving Average)       │
└─────────────────────────────────────────────────────────────────────────┘
```

**Ready for production O2C monitoring!** 🚀
