# O2C Enhanced Platform Dashboard Queries

**Purpose:** Complete Snowsight dashboard queries for O2C Enhanced analytics and monitoring  
**Platform:** Snowsight / Tableau / Power BI  
**Updated:** December 8, 2025  
**Prerequisite:** Run `O2C_ENHANCED_MONITORING_SETUP.sql` first

---

## 📋 Table of Contents

1. [Executive Dashboard](#-executive-dashboard)
2. [Operations Monitoring](#-operations-monitoring)
3. [Data Quality & Freshness](#-data-quality--freshness)
4. [Performance Monitoring](#-performance-monitoring)
5. [Data Load Pattern Analytics](#-data-load-pattern-analytics)
6. [Audit & Tracking](#-audit--tracking)
7. [Alerts & Health](#-alerts--health)
8. [Dashboard Setup Guide](#-dashboard-setup-guide)

---

## 📊 Executive Dashboard

### **TILE 1: O2C Enhanced Executive Scorecard**

**Purpose:** Key business metrics at a glance  
**Type:** Scorecard (8 metrics)  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Enhanced Executive KPI Scorecard
-- ============================================================================
SELECT 
    -- Volume
    total_orders,
    invoiced_orders,
    paid_orders,
    
    -- Values (in thousands)
    ROUND(total_order_value / 1000, 1) AS order_value_k,
    ROUND(total_ar_outstanding / 1000, 1) AS ar_outstanding_k,
    
    -- Performance
    avg_dso,
    median_dso,
    on_time_payment_pct,
    
    -- Conversion
    billing_rate_pct,
    collection_rate_pct,
    
    -- Health
    h.health_status,
    h.health_score
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUSINESS_KPIS k
CROSS JOIN EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY h;
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
    DATE_TRUNC('month', order_date) AS month,
    
    -- Volume
    COUNT(DISTINCT order_key) AS orders,
    COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) AS invoices,
    COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) AS payments,
    
    -- Value
    SUM(order_amount) AS order_value,
    SUM(invoice_amount) AS invoice_value,
    SUM(payment_amount) AS cash_collected,
    
    -- Performance
    ROUND(AVG(days_order_to_cash), 1) AS avg_dso
    
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
WHERE order_date >= DATEADD('month', -12, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;
```

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
    END AS aging_bucket,
    
    COUNT(*) AS invoice_count,
    SUM(outstanding_amount) AS total_outstanding,
    ROUND(AVG(outstanding_amount), 2) AS avg_outstanding
    
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
WHERE reconciliation_status IN ('NOT_PAID', 'OPEN')
  AND invoice_key != 'NOT_INVOICED'
GROUP BY 1
ORDER BY 1;
```

---

## 🔧 Operations Monitoring

### **TILE 4: Daily Execution Summary**

**Purpose:** Track dbt build performance  
**Type:** Multi-line chart  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Daily Build Execution Trend (Last 30 Days)
-- ============================================================================
SELECT 
    execution_date,
    models_run,
    successful_models,
    failed_models,
    total_minutes,
    avg_execution_seconds,
    success_rate_pct
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DAILY_EXECUTION_SUMMARY
ORDER BY execution_date DESC;
```

---

### **TILE 5: Model Performance Ranking**

**Purpose:** Identify optimization opportunities  
**Type:** Horizontal bar chart  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Enhanced Model Performance (Slowest Models)
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
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS;
```

---

### **TILE 6: Recent Model Executions**

**Purpose:** Detailed execution log  
**Type:** Table  
**Refresh:** Real-time

```sql
-- ============================================================================
-- Recent O2C Enhanced Model Executions (Last 24 Hours)
-- ============================================================================
SELECT 
    run_started_at,
    model_name,
    schema_name,
    status,
    ROUND(total_node_runtime, 2) AS execution_seconds,
    rows_affected,
    warehouse_name,
    user_name
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY run_started_at DESC
LIMIT 50;
```

---

## 🔍 Data Quality & Freshness

### **TILE 7: Source Table Freshness**

**Purpose:** Monitor upstream data availability  
**Type:** Table with status indicators  
**Refresh:** Every 30 minutes

```sql
-- ============================================================================
-- O2C Enhanced Source Table Freshness Status
-- ============================================================================
SELECT 
    source_table,
    source_type,
    row_count,
    last_load_timestamp,
    hours_since_load,
    freshness_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SOURCE_FRESHNESS
ORDER BY hours_since_load DESC;
```

---

### **TILE 8: Model Layer Freshness**

**Purpose:** Monitor dbt model freshness  
**Type:** Table with status indicators  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Enhanced Model Layer Freshness Status
-- ============================================================================
SELECT 
    model_name,
    layer,
    row_count,
    last_refresh,
    minutes_since_refresh,
    freshness_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_FRESHNESS
ORDER BY minutes_since_refresh DESC;
```

---

### **TILE 9: Data Flow Row Counts**

**Purpose:** Validate data flow through layers  
**Type:** Table  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Data Flow Validation (Row Counts)
-- ============================================================================
SELECT 
    layer,
    table_name,
    description,
    row_count,
    latest_record,
    checked_at
FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING
ORDER BY 
    CASE layer
        WHEN 'SOURCE' THEN 1
        WHEN 'STAGING' THEN 2
        WHEN 'DIMENSION' THEN 3
        WHEN 'CORE' THEN 4
        WHEN 'EVENTS' THEN 5
        WHEN 'PARTITIONED' THEN 6
        WHEN 'AGGREGATE' THEN 7
    END;
```

---

### **TILE 10: Source to Staging Reconciliation**

**Purpose:** Validate data flows correctly  
**Type:** Table  
**Refresh:** Hourly

```sql
-- ============================================================================
-- Source to Staging Reconciliation
-- ============================================================================
SELECT 
    entity,
    source_rows,
    staging_rows,
    row_variance,
    variance_pct,
    validation_status,
    validated_at
FROM EDW.O2C_AUDIT.V_DATA_FLOW_VALIDATION;
```

---

## ⚡ Performance Monitoring

### **TILE 11: Performance Anomaly Detection**

**Purpose:** Identify models running slower than baseline  
**Type:** Table with severity  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Performance Anomalies
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
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE
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

### **TILE 12: Error Rate Trend**

**Purpose:** Track error rates over time  
**Type:** Line chart with threshold  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Error Rate Trend (Last 30 Days)
-- ============================================================================
SELECT 
    date,
    total_queries,
    error_count,
    success_count,
    error_rate_pct,
    success_rate_pct,
    error_rate_7day_avg
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_TREND
ORDER BY date DESC;
```

---

## 📈 Data Load Pattern Analytics

### **TILE 13: Load Pattern Performance Comparison**

**Purpose:** Compare performance across data load patterns  
**Type:** Bar chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- Load Pattern Performance Comparison
-- ============================================================================
SELECT 
    materialization AS load_pattern,
    model_count,
    total_executions,
    avg_execution_sec,
    max_execution_sec,
    total_rows_processed,
    success_rate_pct
FROM EDW.O2C_AUDIT.V_LOAD_PATTERN_ANALYSIS
ORDER BY total_executions DESC;
```

---

### **TILE 14: Truncate & Load Models**

**Purpose:** Track truncate & load pattern execution  
**Type:** Table  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- Truncate & Load Pattern Models (materialized='table')
-- ============================================================================
SELECT 
    model_name,
    schema_name,
    run_count,
    avg_seconds,
    total_seconds,
    MAX(run_started_at) AS last_run
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_EXECUTIONS
WHERE schema_name LIKE 'O2C_ENHANCED%'
  AND (schema_name LIKE '%DIMENSION%' OR schema_name LIKE '%AGGREGATE%')
  AND run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY model_name, schema_name
ORDER BY avg_seconds DESC;
```

---

### **TILE 15: Incremental Models (Merge/Append/Delete+Insert)**

**Purpose:** Track incremental pattern execution  
**Type:** Table  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- Incremental Pattern Models
-- ============================================================================
SELECT 
    model_name,
    schema_name,
    CASE 
        WHEN schema_name LIKE '%CORE%' THEN 'MERGE'
        WHEN schema_name LIKE '%EVENT%' THEN 'APPEND'
        WHEN schema_name LIKE '%PARTITION%' THEN 'DELETE+INSERT'
        ELSE 'OTHER'
    END AS incremental_strategy,
    COUNT(*) AS run_count,
    ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
    SUM(rows_affected) AS total_rows_processed
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_EXECUTIONS
WHERE schema_name LIKE 'O2C_ENHANCED%'
  AND (schema_name LIKE '%CORE%' OR schema_name LIKE '%EVENT%' OR schema_name LIKE '%PARTITION%')
  AND run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY model_name, schema_name
ORDER BY avg_seconds DESC;
```

---

## 📋 Audit & Tracking

### **TILE 16: Audit Column Validation**

**Purpose:** Verify audit columns are populated  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- Audit Column Validation
-- ============================================================================
SELECT 
    model_name,
    total_rows,
    has_run_id,
    has_batch_id,
    has_loaded_at,
    has_row_hash,
    distinct_runs,
    distinct_batches,
    earliest_load,
    latest_load,
    audit_status
FROM EDW.O2C_AUDIT.V_AUDIT_COLUMN_VALIDATION;
```

---

### **TILE 17: Batch Tracking**

**Purpose:** Track batches across runs  
**Type:** Table  
**Refresh:** Every hour

```sql
-- ============================================================================
-- Batch Tracking (Last 7 Days)
-- ============================================================================
SELECT 
    run_id,
    run_started_at,
    run_status,
    environment,
    model_name,
    batch_id,
    model_status,
    rows_affected,
    execution_seconds,
    materialization
FROM EDW.O2C_AUDIT.V_BATCH_TRACKING
ORDER BY run_started_at DESC
LIMIT 100;
```

---

### **TILE 18: Run Log History**

**Purpose:** Track dbt run history  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- dbt Run Log History
-- ============================================================================
SELECT 
    run_id,
    project_name,
    environment,
    run_started_at,
    run_ended_at,
    run_duration_seconds,
    run_status,
    models_run,
    models_success,
    models_failed,
    warehouse_name,
    user_name
FROM EDW.O2C_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC
LIMIT 50;
```

---

## 🚨 Alerts & Health

### **TILE 19: O2C Enhanced Health Summary**

**Purpose:** Overall platform health  
**Type:** Scorecard  
**Refresh:** Every 5 minutes

```sql
-- ============================================================================
-- O2C Enhanced Platform Health Summary
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
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY;
```

---

### **TILE 20: Active Alerts**

**Purpose:** Current alerts requiring attention  
**Type:** Table  
**Refresh:** Every 5 minutes

```sql
-- ============================================================================
-- O2C Enhanced Active Alerts
-- ============================================================================
-- Performance Alerts
SELECT 
    'PERFORMANCE' AS alert_type,
    severity,
    model_name AS subject,
    'Model running ' || percent_slower || '% slower than baseline' AS description,
    alert_time
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

-- Model Failure Alerts
SELECT 
    'MODEL_FAILURE',
    severity,
    model_name,
    'Model failed ' || failures_last_7_days || ' times in last 7 days',
    failure_time
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_MODEL_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

-- Stale Source Alerts
SELECT 
    'STALE_SOURCE',
    severity,
    source_table,
    alert_description,
    checked_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STALE_SOURCES
WHERE severity IN ('CRITICAL', 'HIGH')

ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    alert_time DESC;
```

---

### **TILE 21: Model Failures Log**

**Purpose:** Track recent failures  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Enhanced Model Failures (Last 7 Days)
-- ============================================================================
SELECT 
    failure_time,
    model_name,
    schema_name,
    warehouse_name,
    execution_seconds,
    failures_last_7_days,
    severity
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_MODEL_FAILURES
ORDER BY failure_time DESC;
```

---

### **TILE 22: Error Log**

**Purpose:** Detailed error information  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Enhanced Error Log (Last 7 Days)
-- ============================================================================
SELECT 
    error_time,
    schema_name,
    error_category,
    error_code,
    LEFT(error_message, 200) AS error_message_preview,
    warehouse_name,
    user_name
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
ORDER BY error_time DESC
LIMIT 50;
```

---

## 🔬 Build & Test Insights

### **TILE 23: Build Failure Details**

**Purpose:** Detailed build failure analysis with error classification  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- O2C Enhanced Build Failure Details
-- ============================================================================
SELECT 
    failure_time,
    affected_object,
    error_category,
    error_code,
    LEFT(error_message, 200) AS error_preview,
    execution_seconds,
    recency_severity,
    warehouse_name,
    user_name
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUILD_FAILURE_DETAILS
ORDER BY failure_time DESC
LIMIT 30;
```

---

### **TILE 24: Test Summary by Type**

**Purpose:** Test health grouped by test type  
**Type:** Table with conditional formatting  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Enhanced Test Summary by Type
-- ============================================================================
SELECT 
    test_type,
    total_executions,
    passed,
    failed,
    pass_rate_pct,
    avg_execution_sec,
    health_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_SUMMARY_BY_TYPE;
```

---

### **TILE 25: Test Pass Rate Trend**

**Purpose:** Track test pass rates over time  
**Type:** Line chart with moving average  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Enhanced Test Pass Rate Trend (Last 30 Days)
-- ============================================================================
SELECT 
    test_date,
    total_tests,
    passed_tests,
    failed_tests,
    pass_rate_pct,
    pass_rate_7day_avg
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_PASS_RATE_TREND
ORDER BY test_date DESC;
```

**Chart Configuration:**
- X-axis: `test_date`
- Y-axes:
  - Primary: `pass_rate_pct` (line)
  - Secondary: `pass_rate_7day_avg` (dashed line)
- Add threshold line at 95%

---

### **TILE 26: Recurring Test Failures**

**Purpose:** Identify persistent test failures  
**Type:** Table with severity  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Enhanced Recurring Test Failures
-- ============================================================================
SELECT 
    test_type,
    test_identifier,
    days_with_failures,
    total_failures,
    first_failure,
    last_failure,
    severity,
    alert_description
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_RECURRING_TEST_FAILURES
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL - Persistent' THEN 1 
        WHEN 'HIGH - Recurring' THEN 2 
        ELSE 3 
    END,
    total_failures DESC;
```

---

## 📊 Event Table Analytics

### **TILE 27: Event Analytics by Type**

**Purpose:** Event distribution and metrics  
**Type:** Table  
**Refresh:** Every 4 hours

```sql
-- ============================================================================
-- O2C Enhanced Event Analytics
-- ============================================================================
SELECT 
    event_type,
    source_system,
    entity_type,
    event_count,
    unique_entities,
    unique_customers,
    total_amount,
    avg_events_per_day
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_EVENT_ANALYTICS
ORDER BY event_count DESC;
```

---

### **TILE 28: Event Timeline**

**Purpose:** Daily event volume  
**Type:** Stacked bar chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Enhanced Event Timeline (Last 30 Days)
-- ============================================================================
SELECT 
    event_date,
    event_type,
    event_count,
    total_amount
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_EVENT_TIMELINE
ORDER BY event_date DESC, event_type;
```

**Chart Configuration:**
- X-axis: `event_date`
- Y-axis: `event_count`
- Stack by: `event_type`

---

## 📈 Data Quality Metrics

### **TILE 29: Row Count Tracking**

**Purpose:** Validate data flow through layers  
**Type:** Table  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Row Count Tracking
-- ============================================================================
SELECT 
    layer,
    table_name,
    description,
    row_count,
    latest_record,
    checked_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ROW_COUNT_TRACKING
ORDER BY 
    CASE layer
        WHEN 'SOURCE' THEN 1
        WHEN 'STAGING' THEN 2
        WHEN 'DIMENSION' THEN 3
        WHEN 'CORE' THEN 4
        WHEN 'EVENTS' THEN 5
        WHEN 'AGGREGATE' THEN 6
    END;
```

---

### **TILE 30: Data Reconciliation**

**Purpose:** Source to staging row count validation  
**Type:** Table with status  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Data Reconciliation
-- ============================================================================
SELECT 
    entity,
    source_rows,
    staging_rows,
    row_variance,
    variance_pct,
    validation_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DATA_RECONCILIATION;
```

---

### **TILE 31: Null Rate Analysis**

**Purpose:** Data quality - null rate monitoring  
**Type:** Table with quality indicators  
**Refresh:** Daily

```sql
-- ============================================================================
-- O2C Enhanced Null Rate Analysis
-- ============================================================================
SELECT 
    table_name,
    column_name,
    total_rows,
    null_count,
    null_rate_pct,
    quality_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_NULL_RATE_ANALYSIS;
```

---

### **TILE 32: Data Completeness Scorecard**

**Purpose:** Overall data completeness metrics  
**Type:** Scorecard  
**Refresh:** Hourly

```sql
-- ============================================================================
-- O2C Enhanced Data Completeness
-- ============================================================================
SELECT 
    dataset,
    total_orders,
    invoice_rate_pct,
    payment_rate_pct,
    customer_enrichment_pct,
    completeness_score,
    quality_grade
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DATA_COMPLETENESS;
```

---

### **TILE 33: Operational Summary**

**Purpose:** Comprehensive operational dashboard  
**Type:** Scorecard (12 metrics)  
**Refresh:** Every 5 minutes

```sql
-- ============================================================================
-- O2C Enhanced Operational Summary
-- ============================================================================
SELECT 
    -- Build Health
    builds_24h,
    successful_builds_24h,
    failed_builds_24h,
    build_success_rate_24h,
    
    -- Test Health
    tests_24h,
    test_pass_rate_24h,
    
    -- Data Quality
    data_completeness_score,
    stale_sources,
    stale_models,
    
    -- Events
    events_24h,
    
    -- Alerts
    critical_alerts,
    platform_health_score,
    platform_health_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_OPERATIONAL_SUMMARY;
```

---

## 🎨 Dashboard Setup Guide

### **Step 1: Run Prerequisites**

```sql
-- 1. First, run the audit setup
@O2C/docs_o2c_enhanced/O2C_ENHANCED_AUDIT_SETUP.sql

-- 2. Run the telemetry setup
@O2C/docs_o2c_enhanced/O2C_ENHANCED_TELEMETRY_SETUP.sql

-- 3. Run the monitoring setup
@O2C/docs_o2c_enhanced/O2C_ENHANCED_MONITORING_SETUP.sql

-- 4. Verify views exist
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY;
SELECT * FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING;
```

### **Step 2: Create Snowsight Dashboard**

1. Log into Snowsight
2. Click **Dashboards** in left navigation
3. Click **+ Dashboard**
4. Name: "O2C Enhanced Analytics Platform"
5. Description: "Order-to-Cash Enhanced monitoring with audit tracking"

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
│  O2C ENHANCED ANALYTICS PLATFORM                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  [TILE 19: Health] [TILE 1: Executive Scorecard - 8 KPIs]  │
│                                                              │
├──────────────────────────┬──────────────────────────────────┤
│  TILE 2: Monthly Trend   │  TILE 3: AR Aging                │
│  (Line Chart)            │  (Bar Chart)                     │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 4: Execution Trend │  TILE 5: Slowest Models          │
│  (Line Chart)            │  (Bar Chart)                     │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 7: Source Fresh    │  TILE 8: Model Fresh             │
│  (Table)                 │  (Table)                         │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 13: Load Patterns  │  TILE 16: Audit Validation       │
│  (Bar Chart)             │  (Table)                         │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 20: Active Alerts  │  TILE 22: Error Log              │
│  (Table)                 │  (Table)                         │
│                          │                                   │
└──────────────────────────┴──────────────────────────────────┘
```

### **Step 5: Configure Alerts**

| Tile | Condition | Frequency | Recipients |
|------|-----------|-----------|------------|
| TILE 19 | health_score < 70 | Immediate | o2c-team@company.com |
| TILE 20 | row_count > 0 | Every 15 min | data-team@company.com |
| TILE 21 | severity = 'CRITICAL' | Immediate | on-call@company.com |
| TILE 7 | Any '❌ Stale' | Every 30 min | data-ops@company.com |

---

## ✅ Summary

**Total Tiles:** 33 dashboard queries across 8 categories

**Coverage:**

| Category | Tiles | Description |
|----------|-------|-------------|
| Executive KPIs | 3 tiles | Business scorecard, trends, AR aging |
| Operations Monitoring | 3 tiles | Execution trend, model performance, recent runs |
| Data Quality & Freshness | 4 tiles | Source freshness, model freshness, row counts, reconciliation |
| Performance | 2 tiles | Anomaly detection, error trend |
| Data Load Patterns | 3 tiles | Pattern comparison, truncate/load, incremental |
| Audit & Tracking | 3 tiles | Audit validation, batch tracking, run log |
| Alerts & Health | 4 tiles | Health summary, active alerts, failures, error log |
| **Build & Test Insights** | **4 tiles** | **Build failures, test summary, pass rate trend, recurring failures** |
| **Event Analytics** | **2 tiles** | **Event analytics, event timeline** |
| **Data Quality Metrics** | **5 tiles** | **Row counts, reconciliation, null rates, completeness, operational summary** |

**Total Monitoring Views:** 25 views in `O2C_ENHANCED_MONITORING` schema

**New Views Added:**
- `O2C_ENH_BUILD_FAILURE_DETAILS` - Extended build failure analysis with error classification
- `O2C_ENH_TEST_SUMMARY_BY_TYPE` - Test results grouped by test type
- `O2C_ENH_TEST_PASS_RATE_TREND` - Daily test pass rate with 7-day moving average
- `O2C_ENH_RECURRING_TEST_FAILURES` - Persistent test failures identification
- `O2C_ENH_EVENT_ANALYTICS` - Event table metrics by type/source
- `O2C_ENH_EVENT_TIMELINE` - Daily event timeline (30 days)
- `O2C_ENH_ROW_COUNT_TRACKING` - Row counts across all layers
- `O2C_ENH_DATA_RECONCILIATION` - Source to staging validation
- `O2C_ENH_NULL_RATE_ANALYSIS` - Null rate for critical columns
- `O2C_ENH_DATA_COMPLETENESS` - Completeness scorecard
- `O2C_ENH_EXECUTION_TIMELINE` - Gantt-style execution timeline
- `O2C_ENH_OPERATIONAL_SUMMARY` - Comprehensive operational dashboard

**Schema Names Used:**
- `EDW.O2C_ENHANCED_MONITORING.*` - Monitoring views (25 views)
- `EDW.O2C_AUDIT.*` - Audit tracking views
- `EDW.O2C_ENHANCED*` - All dbt model schemas (Core, Dimensions, Events, etc.)

**Note:** Schema filtering uses `LIKE 'O2C_ENHANCED%'` pattern to flexibly match all O2C Enhanced schemas.

**Ready for production O2C Enhanced monitoring!** 🚀


