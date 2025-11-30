# Snowsight Dashboard Queries - Complete Observability

## 📊 Overview

This document provides all SQL queries needed to create a comprehensive DBT observability dashboard in Snowflake Snowsight using `dbt_artifacts` package.

**Coverage:**
- ✅ DBT model execution tracking
- ✅ Test results history  
- ✅ Source freshness monitoring
- ✅ Warehouse utilization
- ✅ Performance anomaly detection

## ⚠️ Important: Cost Tracking Limitations

**Snowflake Native DBT Constraint:**
- `QUERY_HISTORY` does NOT have `credits_used` column
- Credits are tracked in `WAREHOUSE_METERING_HISTORY` (warehouse-level, not query-level)
- **Cannot attribute exact credits to individual models**

**Workaround:**
- Cost tiles use `WAREHOUSE_METERING_HISTORY` for warehouse-level credits
- Model-level costs are **estimated** based on execution time
- For accurate per-query costing, use dbt Cloud (not Snowflake Native DBT)

---

## 🚀 Prerequisites

### **1. Install Packages**

```bash
# In both projects
cd dbt_foundation && dbt deps
cd dbt_finance_core && dbt deps
```

### **2. Run DBT (Creates Monitoring Tables)**

```bash
cd dbt_foundation
dbt run
dbt test

cd dbt_finance_core
dbt run
dbt test
```

### **3. Verify Tables Exist**

```sql
-- Check dbt_artifacts tables
SHOW TABLES IN DBT_ARTIFACTS;
-- Expected: MODEL_EXECUTIONS, TEST_EXECUTIONS, SOURCE_FRESHNESS_EXECUTIONS

-- Check Snowflake account usage
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE query_tag LIKE '%dbt%';
```

---

## 📊 Dashboard Tile Queries

### **TILE 1: Executive Summary Scorecard**

**Purpose:** High-level daily metrics at a glance  
**Type:** Scorecard (4-8 metrics)  
**Refresh:** Every 15 minutes  

```sql
-- ============================================================================
-- Executive Summary - Today's Performance
-- ============================================================================
WITH today_runs AS (
    SELECT 
        COUNT(DISTINCT node_id) as models_run,
        SUM(total_node_runtime) as total_seconds,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_models,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed_models
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE DATE(run_started_at) = CURRENT_DATE()
),
today_tests AS (
    SELECT 
        COUNT(*) as total_tests,
        SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed_tests,
        SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed_tests
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS
    WHERE DATE(run_started_at) = CURRENT_DATE()
),
today_costs AS (
    -- Use WAREHOUSE_METERING_HISTORY for accurate credit tracking
    SELECT 
        COALESCE(SUM(credits_used), 0) as total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name IN (
        SELECT DISTINCT warehouse_name 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE query_tag LIKE '%dbt%'
          AND DATE(start_time) = CURRENT_DATE()
    )
    AND DATE(start_time) = CURRENT_DATE()
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
    ROUND(c.total_credits, 2) as daily_cost_credits,
    ROUND(c.total_credits * 3.0, 2) as daily_cost_usd  -- Adjust rate: $3/credit typical
FROM today_runs r
CROSS JOIN today_tests t
CROSS JOIN today_costs c;
```

**Display Options:**
- Create 4 separate scorecard tiles:
  - Models Run Today: `models_run`
  - Test Pass Rate: `test_pass_rate_pct`
  - Execution Time: `total_minutes`
  - Daily Cost: `daily_cost_usd`

---

### **TILE 2: Model Execution Trend (30 Days)**

**Purpose:** Track model execution patterns over time  
**Type:** Multi-line chart  
**Refresh:** Every hour  

```sql
-- ============================================================================
-- Daily Model Execution Trends (Last 30 Days)
-- ============================================================================
SELECT 
    DATE(run_started_at) as execution_date,
    COUNT(DISTINCT node_id) as models_run,
    ROUND(SUM(total_node_runtime) / 60, 1) as total_minutes,
    ROUND(AVG(total_node_runtime), 2) as avg_seconds,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed,
    ROUND(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate_pct
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY execution_date
ORDER BY execution_date DESC;
```

**Chart Configuration:**
- X-axis: `execution_date`
- Y-axes:
  - Primary: `models_run`, `total_minutes`
  - Secondary: `success_rate_pct`
- Chart type: Line chart with multiple series

---

### **TILE 3: Top 10 Slowest Models**

**Purpose:** Identify performance bottlenecks  
**Type:** Horizontal bar chart  
**Refresh:** Every 4 hours  

```sql
-- ============================================================================
-- Slowest Models (Last 7 Days)
-- ============================================================================
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    ROUND(AVG(total_node_runtime), 2) as avg_seconds,
    ROUND(MAX(total_node_runtime), 2) as max_seconds,
    ROUND(MIN(total_node_runtime), 2) as min_seconds,
    ROUND(STDDEV(total_node_runtime), 2) as stddev_seconds,
    CASE 
        WHEN AVG(total_node_runtime) > 300 THEN '🔴 CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN '🟡 SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN '🟢 MODERATE'
        ELSE '⚪ FAST'
    END as performance_tier
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'success'
GROUP BY node_id
ORDER BY avg_seconds DESC
LIMIT 10;
```

**Chart Configuration:**
- Chart type: Horizontal bar
- X-axis: `avg_seconds`
- Y-axis: `model_name`
- Color by: `performance_tier`
- Sort: DESC by `avg_seconds`

---

### **TILE 4: Test Results Health Trend**

**Purpose:** Monitor data quality test pass rates over time  
**Type:** Stacked area chart  
**Refresh:** Every hour  

```sql
-- ============================================================================
-- Test Pass Rate Trend (Last 30 Days)
-- ============================================================================
SELECT 
    DATE(run_started_at) as test_date,
    status,
    COUNT(*) as test_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(run_started_at)), 2) as percentage
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY test_date, status
ORDER BY test_date DESC, status;
```

**Chart Configuration:**
- Chart type: Stacked area chart
- X-axis: `test_date`
- Y-axis: `test_count`
- Series: Group by `status`
- Colors:
  - pass: Green
  - fail: Red
  - warn: Yellow
  - error: Orange

---

### **TILE 5: Daily Cost Trend**

**Purpose:** Track Snowflake credit consumption for DBT workloads  
**Type:** Area chart with line overlay  
**Refresh:** Every hour  

```sql
-- ============================================================================
-- DBT Query Costs Over Time (Last 30 Days)
-- ============================================================================
-- Combine query execution with warehouse credits
WITH dbt_queries AS (
    SELECT 
        DATE(start_time) as query_date,
        warehouse_name,
        COUNT(*) as query_count,
        ROUND(SUM(total_elapsed_time) / 1000 / 60, 1) as total_minutes
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY query_date, warehouse_name
),
warehouse_credits AS (
    SELECT 
        DATE(start_time) as credit_date,
        warehouse_name,
        SUM(credits_used) as total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY credit_date, warehouse_name
)
SELECT 
    q.query_date,
    SUM(q.query_count) as query_count,
    SUM(q.total_minutes) as total_minutes,
    SUM(COALESCE(w.total_credits, 0)) as total_credits,
    ROUND(SUM(COALESCE(w.total_credits, 0)) * 3.0, 2) as estimated_cost_usd,
    ROUND(AVG(COALESCE(w.total_credits, 0)), 4) as avg_credits_per_day
FROM dbt_queries q
LEFT JOIN warehouse_credits w 
    ON q.query_date = w.credit_date 
    AND q.warehouse_name = w.warehouse_name
GROUP BY q.query_date
ORDER BY q.query_date DESC;
```

**Chart Configuration:**
- Chart type: Area chart (fill) + Line chart (overlay)
- X-axis: `query_date`
- Y-axes:
  - Primary (area): `total_credits`
  - Secondary (line): `query_count`

**Alert:** Email when `estimated_cost_usd` > threshold (e.g., $100)

---

### **TILE 6: Most Expensive Models**

**Purpose:** Identify cost optimization opportunities  
**Type:** Horizontal bar chart  
**Refresh:** Every 4 hours  

```sql
-- ============================================================================
-- Most Expensive Models by Execution Time (Last 7 Days)
-- Note: Per-model credits not available in Snowflake Native DBT
-- Using execution time as proxy for cost
-- ============================================================================
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    ROUND(SUM(total_node_runtime), 2) as total_seconds,
    ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes,
    ROUND(AVG(total_node_runtime), 2) as avg_seconds_per_run,
    -- Estimated cost based on execution time
    -- Assumes: X-Small warehouse = $2/hour, adjust as needed
    ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) as estimated_cost_usd,
    CASE 
        WHEN SUM(total_node_runtime) > 3600 THEN '🔴 High Cost'
        WHEN SUM(total_node_runtime) > 600 THEN '🟡 Medium Cost'
        ELSE '🟢 Low Cost'
    END as cost_tier
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'success'
GROUP BY node_id
ORDER BY total_seconds DESC
LIMIT 10;
```

**Chart Configuration:**
- Chart type: Horizontal bar
- X-axis: `estimated_cost_usd`
- Y-axis: `model_name`
- Tooltip: Show `run_count`, `avg_seconds`

---

### **TILE 7: Failed Tests Alert** ⚠️

**Purpose:** Real-time alert for test failures  
**Type:** Table with conditional formatting  
**Refresh:** Every 5 minutes  

```sql
-- ============================================================================
-- Recent Test Failures (Last 24 Hours)
-- ============================================================================
SELECT 
    run_started_at as failure_time,
    SPLIT_PART(node_id, '.', -1) as test_name,
    status,
    failures as failed_row_count,
    SUBSTRING(message, 1, 100) as error_message,
    ROUND(total_node_runtime, 2) as execution_seconds
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND run_started_at >= DATEADD(hour, -24, CURRENT_DATE())
ORDER BY run_started_at DESC;
```

**Alert Configuration:**
- Send email when: `row_count > 0`
- Recipients: data-team@company.com
- Subject: "🚨 DBT Test Failures Detected"
- Frequency: Immediate

**Display:**
- Red highlight for `status = 'error'`
- Orange highlight for `status = 'fail'`

---

### **TILE 8: Model Performance Distribution**

**Purpose:** Understand execution time patterns  
**Type:** Histogram / Bar chart  
**Refresh:** Every 4 hours  

```sql
-- ============================================================================
-- Model Execution Time Distribution (Last 7 Days)
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
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
      AND status = 'success'
)
SELECT 
    time_bucket,
    COUNT(*) as model_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM execution_buckets
GROUP BY time_bucket, bucket_order
ORDER BY bucket_order;
```

**Chart Configuration:**
- Chart type: Vertical bar / Histogram
- X-axis: `time_bucket` (ordered)
- Y-axis: `model_count`
- Show percentage labels

---

### **TILE 9: Warehouse Utilization**

**Purpose:** Track warehouse credit usage by DBT  
**Type:** Stacked bar chart  
**Refresh:** Every 4 hours  

```sql
-- ============================================================================
-- Warehouse Credit Usage (Last 7 Days)
-- Uses WAREHOUSE_METERING_HISTORY for accurate credit tracking
-- ============================================================================
SELECT 
    DATE(start_time) as usage_date,
    warehouse_name,
    ROUND(SUM(credits_used), 3) as total_credits,
    ROUND(SUM(credits_used) * 3.0, 2) as estimated_cost_usd,
    ROUND(SUM(credits_used_compute), 3) as compute_credits,
    ROUND(SUM(credits_used_cloud_services), 3) as cloud_services_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY usage_date, warehouse_name
ORDER BY usage_date DESC, total_credits DESC;
```

**Chart Configuration:**
- Chart type: Stacked bar
- X-axis: `usage_date`
- Y-axis: `total_credits`
- Stack by: `warehouse_name`
- Colors: One per warehouse

---

### **TILE 10: Source Freshness Status**

**Purpose:** Monitor data source freshness  
**Type:** Table with status indicators  
**Refresh:** Every 15 minutes  

```sql
-- ============================================================================
-- Source Freshness Checks (Most Recent)
-- ============================================================================
SELECT 
    SPLIT_PART(node_id, '.', -1) as source_name,
    status,
    max_loaded_at as last_data_timestamp,
    snapshotted_at as check_timestamp,
    DATEDIFF('hour', max_loaded_at, snapshotted_at) as hours_since_last_load,
    CASE 
        WHEN status = 'pass' THEN '✅ Fresh'
        WHEN status = 'warn' THEN '⚠️ Warning'
        WHEN status = 'error' THEN '❌ Stale'
        ELSE '❓ Unknown'
    END as freshness_status
FROM DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
QUALIFY ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY run_started_at DESC) = 1
ORDER BY hours_since_last_load DESC;
```

**Display:**
- Conditional formatting by `status`
- Alert when any source has `status = 'error'`

---

### **TILE 11: Performance Anomalies** ⚠️

**Purpose:** Detect models running slower than usual  
**Type:** Alert table  
**Refresh:** Every hour  

```sql
-- ============================================================================
-- Models Running Slower Than Baseline (Statistical Anomaly Detection)
-- ============================================================================
WITH baseline AS (
    -- Baseline: 7-14 days ago
    SELECT 
        node_id,
        AVG(total_node_runtime) as baseline_avg,
        STDDEV(total_node_runtime) as baseline_stddev
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'success'
    GROUP BY node_id
),
recent AS (
    -- Recent: Last 24 hours
    SELECT 
        node_id,
        AVG(total_node_runtime) as recent_avg
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -1, CURRENT_DATE())
      AND status = 'success'
    GROUP BY node_id
)
SELECT 
    SPLIT_PART(r.node_id, '.', -1) as model_name,
    ROUND(b.baseline_avg, 2) as baseline_seconds,
    ROUND(r.recent_avg, 2) as recent_seconds,
    ROUND(r.recent_avg - b.baseline_avg, 2) as seconds_slower,
    ROUND((r.recent_avg - b.baseline_avg) / b.baseline_avg * 100, 1) as percent_slower,
    CASE 
        WHEN r.recent_avg > b.baseline_avg + (3 * b.baseline_stddev) THEN '🔴 Critical'
        WHEN r.recent_avg > b.baseline_avg + (2 * b.baseline_stddev) THEN '🟡 Warning'
        ELSE '🟢 Normal'
    END as severity
FROM recent r
JOIN baseline b ON r.node_id = b.node_id
WHERE r.recent_avg > b.baseline_avg + (2 * b.baseline_stddev)  -- 2 sigma threshold
ORDER BY percent_slower DESC;
```

**Alert Configuration:**
- Email when: `row_count > 0`
- Subject: "⚠️ DBT Performance Degradation Detected"

---

### **TILE 12: Test Coverage by Model**

**Purpose:** Monitor test coverage and health per model  
**Type:** Table with health scores  
**Refresh:** Every 4 hours  

```sql
-- ============================================================================
-- Test Coverage Heatmap (Last 7 Days)
-- ============================================================================
WITH model_tests AS (
    SELECT 
        -- Extract model name from test node_id
        SPLIT_PART(SPLIT_PART(node_id, '.', 4), '_', 1) as model_name,
        COUNT(DISTINCT node_id) as test_count,
        SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed,
        SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) as warned
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY model_name
)
SELECT 
    model_name,
    test_count,
    passed,
    failed,
    warned,
    ROUND(passed * 100.0 / test_count, 1) as pass_rate_pct,
    CASE 
        WHEN pass_rate_pct = 100 THEN '✅ Excellent'
        WHEN pass_rate_pct >= 95 THEN '✅ Good'
        WHEN pass_rate_pct >= 90 THEN '⚠️ Fair'
        ELSE '❌ Poor'
    END as health_status
FROM model_tests
WHERE test_count > 0
ORDER BY pass_rate_pct ASC, model_name;
```

**Display:**
- Color-code by `health_status`
- Highlight models with `pass_rate_pct < 95`

---

### **TILE 13: Cost Optimization Opportunities**

**Purpose:** Identify high-cost, low-efficiency models  
**Type:** Scatter plot or prioritized table  
**Refresh:** Daily  

```sql
-- ============================================================================
-- Models to Optimize (Slowest + Most Resource Intensive)
-- Note: Using execution time as proxy for cost (exact credits not available per-model)
-- ============================================================================
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    ROUND(SUM(total_node_runtime), 2) as total_seconds,
    ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes,
    ROUND(AVG(total_node_runtime), 2) as avg_seconds_per_run,
    ROUND(rows_affected, 0) as avg_rows_affected,
    -- Estimated cost based on execution time
    -- Formula: (total_seconds / 3600) * warehouse_cost_per_hour
    -- Assumes X-Small warehouse ($2/hour), adjust based on your warehouse size
    ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) as estimated_cost_usd,
    CASE 
        WHEN SUM(total_node_runtime) > 3600 THEN '🔴 High Priority'
        WHEN SUM(total_node_runtime) > 600 THEN '🟡 Medium Priority'
        ELSE '🟢 Low Priority'
    END as optimization_priority
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'success'
GROUP BY node_id, rows_affected
HAVING SUM(total_node_runtime) > 10  -- Focus on models taking >10 seconds total
ORDER BY total_seconds DESC
LIMIT 20;
```

**Display:**
- Sort by `optimization_priority` then `estimated_cost_usd`
- Highlight High Priority items

---

### **TILE 14: Week-over-Week Comparison**

**Purpose:** Track key metrics week-over-week  
**Type:** Comparison table  
**Refresh:** Daily  

```sql
-- ============================================================================
-- Week-over-Week Performance Comparison
-- ============================================================================
WITH this_week AS (
    SELECT 
        COUNT(DISTINCT node_id) as models,
        ROUND(SUM(total_node_runtime) / 60, 1) as total_minutes,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failures
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at >= DATE_TRUNC('week', CURRENT_DATE())
),
last_week AS (
    SELECT 
        COUNT(DISTINCT node_id) as models,
        ROUND(SUM(total_node_runtime) / 60, 1) as total_minutes,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failures
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(week, -1, DATE_TRUNC('week', CURRENT_DATE()))
      AND run_started_at < DATE_TRUNC('week', CURRENT_DATE())
),
this_week_cost AS (
    SELECT ROUND(SUM(credits_used), 2) as credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATE_TRUNC('week', CURRENT_DATE())
),
last_week_cost AS (
    SELECT ROUND(SUM(credits_used), 2) as credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD(week, -1, DATE_TRUNC('week', CURRENT_DATE()))
      AND start_time < DATE_TRUNC('week', CURRENT_DATE())
)
SELECT 
    'This Week' as period,
    tw.models,
    tw.total_minutes,
    tw.failures,
    twc.credits as total_credits
FROM this_week tw, this_week_cost twc
UNION ALL
SELECT 
    'Last Week',
    lw.models,
    lw.total_minutes,
    lw.failures,
    lwc.credits
FROM last_week lw, last_week_cost lwc
UNION ALL
SELECT 
    'Change (%)',
    ROUND((tw.models - lw.models) * 100.0 / NULLIF(lw.models, 0), 1),
    ROUND((tw.total_minutes - lw.total_minutes) * 100.0 / NULLIF(lw.total_minutes, 0), 1),
    ROUND((tw.failures - lw.failures) * 100.0 / NULLIF(lw.failures, 0), 1),
    ROUND((twc.credits - lwc.credits) * 100.0 / NULLIF(lwc.credits, 0), 1)
FROM this_week tw, last_week lw, this_week_cost twc, last_week_cost lwc;
```

**Display:**
- 3-row comparison table
- Color-code positive/negative changes

---

## 🎨 Recommended Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  DBT OBSERVABILITY DASHBOARD - PRODUCTION                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  [TILE 1: Executive Summary - 4 Scorecards in Row]         │
│  Models | Test Pass Rate | Execution Time | Daily Cost     │
│                                                              │
├──────────────────────────┬──────────────────────────────────┤
│  TILE 2: Execution Trend │  TILE 3: Slowest Models          │
│  (Line Chart - 30 days)  │  (Bar Chart - Top 10)            │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 4: Test Health     │  TILE 5: Cost Trend              │
│  (Stacked Area)          │  (Area Chart - 30 days)          │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 6: Expensive Models│  TILE 7: Failed Tests ⚠️         │
│  (Bar Chart - Top 10)    │  (Alert Table - Real-time)       │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 8: Time Distribution│ TILE 9: Warehouse Usage         │
│  (Histogram)             │  (Stacked Bar)                   │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 10: Source Freshness│ TILE 11: Performance Anomalies │
│  (Table with status)     │  (Alert Table)                   │
│                          │                                   │
├──────────────────────────┼──────────────────────────────────┤
│  TILE 12: Test Coverage  │  TILE 13: Optimization Ideas     │
│  (Table - Health Score)  │  (Priority Table)                │
│                          │                                   │
├──────────────────────────┴──────────────────────────────────┤
│  TILE 14: Weekly Summary (3-row Comparison Table)          │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Dashboard Configuration

### **Refresh Schedules:**

| Tile Category | Refresh Interval | Reason |
|---------------|------------------|--------|
| **Real-time alerts** (7, 11) | Every 5 minutes | Immediate notification |
| **Executive summary** (1) | Every 15 minutes | Leadership visibility |
| **Operational** (2, 4, 5, 10) | Every hour | Trend monitoring |
| **Analysis** (3, 6, 8, 9, 12, 13) | Every 4 hours | Performance analysis |
| **Summary** (14) | Daily | Week-over-week tracking |

---

### **Alert Configuration:**

#### **Alert 1: Test Failures (Tile 7)**
```
Condition: Row count > 0
Recipients: data-team@company.com
Subject: 🚨 DBT Test Failures Detected
Frequency: Immediate
Priority: High
```

#### **Alert 2: Performance Degradation (Tile 11)**
```
Condition: Row count > 0
Recipients: data-engineering@company.com
Subject: ⚠️ DBT Performance Degradation
Frequency: Hourly digest
Priority: Medium
```

#### **Alert 3: Daily Cost Threshold (Tile 5)**
```
Condition: daily_cost_usd > $100
Recipients: finance@company.com, data-team@company.com
Subject: 💰 DBT Daily Cost Alert
Frequency: Daily at 5pm
Priority: Medium
```

#### **Alert 4: Source Staleness (Tile 10)**
```
Condition: Any source with status = 'error'
Recipients: data-ops@company.com
Subject: ⚠️ Data Source Freshness Alert
Frequency: Every 15 minutes
Priority: High
```

---

## 📊 Creating the Dashboard in Snowsight

### **Step 1: Create New Dashboard**

```
1. Log into Snowsight
2. Click "Dashboards" in left navigation
3. Click "+ Dashboard" button
4. Name: "DBT Observability - Production"
5. Description: "Complete observability for DBT projects"
```

### **Step 2: Add Tiles**

For each tile above:

```
1. Click "+ Add Tile"
2. Select "From SQL Query"
3. Paste the SQL query
4. Click "Run"
5. Configure visualization:
   - Chart type (as specified above)
   - X/Y axes
   - Series/colors
6. Set tile title
7. Configure refresh schedule
8. Save tile
```

### **Step 3: Arrange Layout**

```
1. Drag tiles to match recommended layout
2. Resize tiles as needed
3. Group related tiles together
4. Save dashboard
```

### **Step 4: Set Up Alerts**

```
1. Click tile menu (...)
2. Select "Set Alert"
3. Configure condition
4. Add recipients
5. Set frequency
6. Save alert
```

### **Step 5: Share Dashboard**

```
1. Click "Share" button
2. Add users/roles
3. Set permissions (View only / Edit)
4. Copy dashboard link
5. Share with team
```

---

## 🔍 Troubleshooting

### **Issue: Tables Don't Exist**

```sql
-- Verify dbt_artifacts tables
SHOW TABLES IN DBT_ARTIFACTS;

-- If empty, run:
cd dbt_foundation
dbt run
dbt test
```

### **Issue: No Data in Tables**

```sql
-- Check if any data exists
SELECT COUNT(*) FROM DBT_ARTIFACTS.MODEL_EXECUTIONS;

-- If 0, run dbt again to populate
dbt run
dbt test
```

### **Issue: Query History Empty**

```sql
-- Snowflake ACCOUNT_USAGE has latency (up to 45 min)
-- Check if any dbt queries exist
SELECT COUNT(*) 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%';

-- If 0, wait or check INFORMATION_SCHEMA instead
SELECT COUNT(*) 
FROM SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%';
```

### **Issue: Permission Denied**

```sql
-- Grant required permissions
GRANT USAGE ON DATABASE DBT_ARTIFACTS TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DBT_ARTIFACTS TO ROLE DBT_DEV_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DBT_DEV_ROLE;
```

---

## 💡 Best Practices

### **Dashboard Maintenance:**

1. **Review Weekly**
   - Check for broken queries
   - Validate alert thresholds
   - Update cost estimates

2. **Optimize Queries**
   - Add indexes if queries slow
   - Use materialized views for complex calculations
   - Consider caching frequently-accessed data

3. **Monitor Dashboard Usage**
   ```sql
   SELECT 
       user_name,
       COUNT(*) as view_count
   FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
   WHERE object_name = 'DBT Observability - Production'
   GROUP BY user_name;
   ```

4. **Version Control**
   - Save queries to Git
   - Document changes
   - Test before deploying to production

---

## 📚 Additional Resources

- **dbt_artifacts package:** https://github.com/brooklyn-data/dbt_artifacts
- **dbt-snowflake-monitoring:** https://github.com/get-select/dbt-snowflake-monitoring
- **Snowsight documentation:** https://docs.snowflake.com/en/user-guide/ui-snowsight
- **Snowflake ACCOUNT_USAGE views:** https://docs.snowflake.com/en/sql-reference/account-usage

---

## ✅ Summary

**You now have:**
- ✅ 14 comprehensive dashboard tiles
- ✅ Complete observability coverage
- ✅ Cost tracking and optimization
- ✅ Performance anomaly detection
- ✅ Proactive alerting
- ✅ Test quality monitoring
- ✅ Source freshness tracking

**Total Coverage:**
- Model execution tracking
- Test results history
- Source freshness monitoring
- Query costs & warehouse utilization
- Performance anomalies
- Week-over-week trends

**Ready to deploy your production DBT observability dashboard!** 📊🚀

---

# 🚨 COMPREHENSIVE ALERTS & MONITORING DASHBOARD

This section contains enhanced queries for comprehensive alerting, monitoring, and notifications.

**Prerequisites:**
- Run `setup_comprehensive_alerts.sql` to create all alert views
- Run `setup_notifications.sql` to configure automated notifications
- All packages installed (`dbt deps` in both projects)

---

## 📈 SECTION 7: COMPREHENSIVE ALERT DASHBOARD

### **TILE 7.1: Alert Summary Scorecard**

**Purpose:** Real-time overview of all alert categories  
**Type:** Scorecard  
**Refresh:** Every 15 minutes  

```sql
-- ============================================================================
-- Alert Summary Dashboard - Real-Time Health Overview
-- ============================================================================
SELECT 
    -- Overall Health
    health_score,
    CASE 
        WHEN health_score >= 90 THEN '✅ EXCELLENT'
        WHEN health_score >= 75 THEN '⚠️ GOOD'
        WHEN health_score >= 50 THEN '⚠️ WARNING'
        ELSE '🚨 CRITICAL'
    END as health_status,
    
    -- Test Failures
    critical_test_failures,
    high_test_failures,
    recurring_test_failures,
    
    -- Performance
    critical_performance_issues,
    high_performance_issues,
    model_failures,
    
    -- Data Quality
    stale_sources,
    missing_data_loads,
    
    -- Cost & Resources
    cost_spikes,
    expensive_queries,
    warehouse_queuing_issues,
    
    -- SLA
    sla_violations,
    
    -- Timestamp
    snapshot_time as last_updated
FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;
```

**Display As:** Scorecard with conditional formatting:
- Green: health_score >= 90
- Yellow: health_score 50-89
- Red: health_score < 50

---

### **TILE 7.2: Critical Alerts - Action Required**

**Purpose:** All critical and high-severity alerts requiring immediate attention  
**Type:** Table  
**Refresh:** Every 5 minutes  
**Alert:** Send email when count > 0  

```sql
-- ============================================================================
-- Critical Alerts - Immediate Action Required
-- ============================================================================
SELECT 
    alert_category,
    severity,
    alert_subject,
    alert_description,
    alert_time,
    -- Priority for sorting
    CASE severity 
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END as priority,
    -- Time since alert
    DATEDIFF('minute', alert_time, CURRENT_TIMESTAMP()) as minutes_ago
FROM DBT_MONITORING.ALERT_ALL_CRITICAL
WHERE severity IN ('CRITICAL', 'HIGH')
ORDER BY priority, alert_time DESC
LIMIT 20;
```

**Snowsight Configuration:**
- Enable table alerts
- Condition: Row count > 0
- Recipients: data-team@company.com
- Frequency: Immediate

---

### **TILE 7.3: Test Failure Trends**

**Purpose:** Track test failures over time  
**Type:** Stacked Area Chart  
**Refresh:** Hourly  

```sql
-- ============================================================================
-- Test Failure Trends - Last 30 Days
-- ============================================================================
SELECT 
    DATE_TRUNC('day', generated_at) as test_date,
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'Uniqueness Tests'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'Not Null Tests'
        WHEN LOWER(node_id) LIKE '%relationships%' THEN 'Relationship Tests'
        WHEN LOWER(node_id) LIKE '%accepted_values%' THEN 'Accepted Values Tests'
        ELSE 'Other Tests'
    END as test_category,
    COUNT(*) as failure_count
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND generated_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**Chart Configuration:**
- X-axis: test_date
- Y-axis: failure_count (stacked)
- Group by: test_category
- Colors: Red gradient

---

### **TILE 7.4: Performance Degradation Monitor**

**Purpose:** Models running slower than baseline  
**Type:** Bar Chart (Horizontal)  
**Refresh:** Every 30 minutes  

```sql
-- ============================================================================
-- Performance Degradation - Models Running Slow
-- ============================================================================
SELECT 
    model_name,
    ROUND(baseline_seconds, 1) as baseline_seconds,
    ROUND(recent_avg_seconds, 1) as current_seconds,
    ROUND(percent_slower, 1) as percent_slower,
    severity,
    recent_run_count
FROM DBT_MONITORING.ALERT_CRITICAL_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
ORDER BY percent_slower DESC
LIMIT 15;
```

**Chart Configuration:**
- Horizontal bar chart
- X-axis: percent_slower
- Y-axis: model_name
- Color by: severity (Red=CRITICAL, Orange=HIGH, Yellow=MEDIUM)

---

### **TILE 7.5: Data Freshness Heat Map**

**Purpose:** Visual representation of data staleness  
**Type:** Table with conditional formatting  
**Refresh:** Every 30 minutes  

```sql
-- ============================================================================
-- Data Freshness Status - Source Staleness
-- ============================================================================
SELECT 
    source_name,
    max_loaded_at,
    hours_stale,
    CASE 
        WHEN days_stale >= 1 THEN days_stale || ' days'
        ELSE hours_stale || ' hours'
    END as staleness,
    severity,
    alert_description,
    checked_at
FROM DBT_MONITORING.ALERT_STALE_SOURCES
ORDER BY hours_stale DESC;
```

**Conditional Formatting:**
- Red background: severity = 'CRITICAL' (>72 hours)
- Orange background: severity = 'HIGH' (>48 hours)
- Yellow background: severity = 'MEDIUM' (>24 hours)

---

### **TILE 7.6: Cost Tracking & Anomalies**

**Purpose:** Daily cost trends with spike detection  
**Type:** Dual-axis Line Chart  
**Refresh:** Daily  

```sql
-- ============================================================================
-- Cost Trends with Anomaly Detection
-- ============================================================================
WITH daily_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as cost_date,
        SUM(COALESCE(
            CASE 
                WHEN execution_time > 0 
                THEN (execution_time/1000) * 0.0006 -- Estimated cost per second
                ELSE 0 
            END, 0)
        ) as estimated_credits,
        COUNT(*) as query_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1
),
cost_stats AS (
    SELECT 
        AVG(estimated_credits) as avg_credits,
        STDDEV(estimated_credits) as stddev_credits
    FROM daily_costs
    WHERE cost_date >= DATEADD(day, -14, CURRENT_DATE())
)
SELECT 
    d.cost_date,
    ROUND(d.estimated_credits, 4) as daily_credits,
    d.query_count,
    ROUND(s.avg_credits, 4) as baseline_avg,
    ROUND(s.avg_credits + (2 * s.stddev_credits), 4) as alert_threshold,
    CASE 
        WHEN d.estimated_credits > s.avg_credits + (2 * s.stddev_credits) THEN 'SPIKE'
        ELSE 'NORMAL'
    END as status
FROM daily_costs d
CROSS JOIN cost_stats s
ORDER BY d.cost_date DESC;
```

**Chart Configuration:**
- Line 1: daily_credits (solid blue line)
- Line 2: baseline_avg (dashed green line)
- Line 3: alert_threshold (dashed red line)
- Markers: Red dots where status = 'SPIKE'

---

### **TILE 7.7: Model Failure Analysis**

**Purpose:** Track model failures and recurring issues  
**Type:** Table  
**Refresh:** Every 15 minutes  

```sql
-- ============================================================================
-- Model Failure Analysis - Recurring Issues
-- ============================================================================
SELECT 
    model_name,
    generated_at as last_failure,
    status,
    failure_count_last_7_days,
    severity,
    alert_description,
    LEFT(message, 200) || '...' as error_preview
FROM DBT_MONITORING.ALERT_MODEL_FAILURES
ORDER BY failure_count_last_7_days DESC, generated_at DESC
LIMIT 20;
```

**Alert Configuration:**
- Email alert when failure_count_last_7_days >= 3
- Recipients: data-engineering@company.com

---

### **TILE 7.8: SLA Compliance Dashboard**

**Purpose:** Track model execution against SLA thresholds  
**Type:** Gauge Chart  
**Refresh:** Every 30 minutes  

```sql
-- ============================================================================
-- SLA Compliance - Model Execution Performance
-- ============================================================================
WITH all_sla_models AS (
    SELECT 
        model_name,
        actual_seconds,
        sla_seconds,
        percent_over_sla,
        severity,
        generated_at
    FROM DBT_MONITORING.ALERT_SLA_VIOLATIONS
    WHERE generated_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
),
sla_summary AS (
    SELECT 
        COUNT(*) as total_runs,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_violations,
        SUM(CASE WHEN severity IN ('CRITICAL', 'HIGH') THEN 1 ELSE 0 END) as total_violations,
        AVG(percent_over_sla) as avg_overage_pct
    FROM all_sla_models
)
SELECT 
    total_runs,
    total_violations,
    critical_violations,
    ROUND((total_runs - total_violations)::FLOAT / NULLIF(total_runs, 0) * 100, 1) as sla_compliance_pct,
    ROUND(avg_overage_pct, 1) as avg_overage_pct
FROM sla_summary;
```

**Gauge Configuration:**
- Metric: sla_compliance_pct
- Green: 95-100%
- Yellow: 85-94%
- Red: <85%

---

### **TILE 7.9: Long-Running Queries**

**Purpose:** Identify queries consuming excessive time  
**Type:** Table  
**Refresh:** Hourly  

```sql
-- ============================================================================
-- Long-Running Queries - Performance Bottlenecks
-- ============================================================================
SELECT 
    warehouse_name,
    database_name,
    ROUND(execution_minutes, 2) as exec_minutes,
    rows_produced,
    ROUND(bytes_scanned / POWER(1024, 3), 2) as gb_scanned,
    severity,
    alert_description,
    start_time,
    query_id
FROM DBT_MONITORING.ALERT_LONG_RUNNING_QUERIES
WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
ORDER BY execution_minutes DESC
LIMIT 20;
```

---

### **TILE 7.10: Warehouse Queuing Issues**

**Purpose:** Detect warehouse capacity issues  
**Type:** Table  
**Refresh:** Every 30 minutes  

```sql
-- ============================================================================
-- Warehouse Queuing - Capacity Issues
-- ============================================================================
SELECT 
    warehouse_name,
    ROUND(total_queue_seconds / 60, 2) as queue_minutes,
    ROUND(execution_seconds, 1) as exec_seconds,
    ROUND(total_queue_seconds / NULLIF(execution_seconds, 0) * 100, 1) as queue_pct_of_exec,
    severity,
    alert_description,
    start_time,
    user_name
FROM DBT_MONITORING.ALERT_WAREHOUSE_QUEUING
WHERE severity IN ('CRITICAL', 'HIGH')
ORDER BY total_queue_seconds DESC
LIMIT 15;
```

**Alert:** Notify when queue_minutes > 5

---

### **TILE 7.11: Recurring Test Failures**

**Purpose:** Identify systemic data quality issues  
**Type:** Table  
**Refresh:** Daily  

```sql
-- ============================================================================
-- Recurring Test Failures - Systemic Issues
-- ============================================================================
SELECT 
    test_name,
    failure_count,
    days_failing,
    first_failure,
    last_failure,
    severity,
    alert_description
FROM DBT_MONITORING.ALERT_RECURRING_TEST_FAILURES
ORDER BY failure_count DESC, last_failure DESC;
```

**Highlight:** Tests with failure_count >= 5 in red

---

### **TILE 7.12: Test Pass Rate Degradation**

**Purpose:** Monitor overall data quality trends  
**Type:** KPI with trend line  
**Refresh:** Daily  

```sql
-- ============================================================================
-- Test Pass Rate - Quality Trend
-- ============================================================================
SELECT 
    alert_date,
    today_pass_rate,
    baseline_pass_rate,
    pass_rate_change,
    total_tests,
    passed_tests,
    failed_tests,
    severity,
    alert_description
FROM DBT_MONITORING.ALERT_TEST_PASS_RATE_DROP
WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
ORDER BY alert_date DESC;
```

---

### **TILE 7.13: Expensive Queries Monitor**

**Purpose:** Track queries consuming excessive credits  
**Type:** Table  
**Refresh:** Every 4 hours  

```sql
-- ============================================================================
-- Expensive Queries - Cost Optimization Targets
-- ============================================================================
SELECT 
    warehouse_name,
    database_name,
    ROUND(execution_seconds, 1) as exec_seconds,
    ROUND(credits_used, 4) as credits,
    ROUND(bytes_scanned / POWER(1024, 3), 2) as gb_scanned,
    rows_produced,
    severity,
    start_time,
    query_id,
    query_text_preview
FROM DBT_MONITORING.ALERT_EXPENSIVE_QUERIES
WHERE severity IN ('CRITICAL', 'HIGH')
ORDER BY credits DESC
LIMIT 20;
```

**Use Case:** Prioritize optimization efforts on queries with highest credits

---

### **TILE 7.14: Missing Data Loads**

**Purpose:** Detect sources with expected regular loads that are missing  
**Type:** Table with time indicators  
**Refresh:** Every 6 hours  

```sql
-- ============================================================================
-- Missing Data Loads - Expected but Not Received
-- ============================================================================
SELECT 
    source_name,
    last_load_time,
    hours_since_last_load,
    ROUND(hours_since_last_load / 24.0, 1) as days_since_load,
    load_days_last_week,
    severity,
    alert_description
FROM DBT_MONITORING.ALERT_MISSING_DATA_LOADS
ORDER BY hours_since_last_load DESC;
```

**Conditional Formatting:**
- Critical: hours_since_last_load > 48
- High: hours_since_last_load > 36
- Medium: hours_since_last_load > 24

---

## 📧 SECTION 8: NOTIFICATION & ALERT MANAGEMENT

### **TILE 8.1: Alert Audit Log**

**Purpose:** Track all alerts generated and notifications sent  
**Type:** Table  
**Refresh:** Real-time  

```sql
-- ============================================================================
-- Alert Audit Log - Notification History
-- ============================================================================
SELECT 
    alert_id,
    alert_timestamp,
    alert_category,
    severity,
    alert_subject,
    LEFT(alert_description, 100) || '...' as description_preview,
    notification_sent,
    notification_method,
    acknowledged,
    acknowledged_by,
    acknowledged_at,
    DATEDIFF('minute', alert_timestamp, COALESCE(acknowledged_at, CURRENT_TIMESTAMP())) as minutes_to_ack
FROM DBT_MONITORING.ALERT_AUDIT_LOG
ORDER BY alert_timestamp DESC
LIMIT 100;
```

**Filter:** Show only unacknowledged alerts (acknowledged = FALSE)

---

### **TILE 8.2: Task Execution Status**

**Purpose:** Monitor scheduled task health  
**Type:** Table  
**Refresh:** Every 15 minutes  

```sql
-- ============================================================================
-- Task Execution Status - Monitoring Automation Health
-- ============================================================================
SELECT 
    task_name,
    state,
    schedule,
    warehouse,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON,
    LAST_ERROR_MESSAGE,
    NEXT_SCHEDULED_TIME,
    DATEDIFF('minute', LAST_COMMITTED_ON, CURRENT_TIMESTAMP()) as minutes_since_last_run
FROM DBT_MONITORING.TASK_STATUS_MONITORING
ORDER BY 
    CASE state 
        WHEN 'suspended' THEN 1
        WHEN 'failed' THEN 2
        ELSE 3
    END,
    task_name;
```

**Alert:** Email when state = 'suspended' or LAST_ERROR_MESSAGE IS NOT NULL

---

### **TILE 8.3: Daily Metrics Summary**

**Purpose:** Comprehensive daily rollup for morning review  
**Type:** Scorecard  
**Refresh:** Daily at 8 AM  

```sql
-- ============================================================================
-- Daily Summary - Morning Review Dashboard
-- ============================================================================
WITH yesterday_summary AS (
    SELECT 
        DATE(run_started_at) as report_date,
        COUNT(DISTINCT node_id) as models_run,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_models,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed_models,
        ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes,
        ROUND(AVG(total_node_runtime), 1) as avg_seconds
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE DATE(run_started_at) = CURRENT_DATE() - 1
    GROUP BY 1
),
yesterday_tests AS (
    SELECT 
        COUNT(*) as total_tests,
        SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed_tests,
        ROUND(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100, 1) as pass_rate
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS
    WHERE DATE(run_started_at) = CURRENT_DATE() - 1
),
yesterday_alerts AS (
    SELECT 
        COUNT(*) as total_alerts,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_alerts
    FROM DBT_MONITORING.ALERT_AUDIT_LOG
    WHERE DATE(alert_timestamp) = CURRENT_DATE() - 1
)
SELECT 
    s.report_date,
    s.models_run,
    s.successful_models,
    s.failed_models,
    s.total_minutes,
    s.avg_seconds,
    t.total_tests,
    t.passed_tests,
    t.pass_rate,
    a.total_alerts,
    a.critical_alerts,
    CASE 
        WHEN s.failed_models = 0 AND t.pass_rate >= 95 AND a.critical_alerts = 0 THEN '✅ EXCELLENT'
        WHEN s.failed_models <= 1 AND t.pass_rate >= 90 AND a.critical_alerts <= 2 THEN '⚠️ GOOD'
        WHEN s.failed_models <= 3 AND t.pass_rate >= 80 AND a.critical_alerts <= 5 THEN '⚠️ WARNING'
        ELSE '🚨 NEEDS ATTENTION'
    END as daily_health_status
FROM yesterday_summary s
CROSS JOIN yesterday_tests t
CROSS JOIN yesterday_alerts a;
```

---

## 🎯 SECTION 9: ADVANCED MONITORING QUERIES

### **QUERY 9.1: Model Execution Trends (7-Day Moving Average)**

```sql
-- ============================================================================
-- Model Performance Trends - Moving Averages
-- ============================================================================
SELECT 
    execution_date,
    model_name,
    ROUND(avg_execution_seconds, 2) as daily_avg_seconds,
    ROUND(moving_avg_7day, 2) as seven_day_avg_seconds,
    ROUND((avg_execution_seconds - moving_avg_7day) / NULLIF(moving_avg_7day, 0) * 100, 1) as pct_vs_trend
FROM DBT_MONITORING.MODEL_EXECUTION_TRENDS
WHERE model_name IN (
    -- Top 10 most frequently run models
    SELECT SPLIT_PART(node_id, '.', -1)
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY 1
    ORDER BY COUNT(*) DESC
    LIMIT 10
)
  AND execution_date >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY execution_date DESC, model_name;
```

---

### **QUERY 9.2: Query Pattern Analysis**

```sql
-- ============================================================================
-- Query Pattern Analysis - Execution Patterns
-- ============================================================================
SELECT 
    HOUR(run_started_at) as execution_hour,
    COUNT(DISTINCT node_id) as unique_models,
    COUNT(*) as total_executions,
    ROUND(AVG(total_node_runtime), 1) as avg_runtime_seconds,
    ROUND(SUM(total_node_runtime) / 60, 2) as total_runtime_minutes
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'success'
GROUP BY 1
ORDER BY 1;
```

**Use Case:** Identify peak execution hours for warehouse sizing

---

### **QUERY 9.3: Test Coverage Analysis**

```sql
-- ============================================================================
-- Test Coverage - Model Quality Assurance
-- ============================================================================
WITH model_test_counts AS (
    SELECT 
        SPLIT_PART(te.node_id, '.', 2) as model_name,
        COUNT(DISTINCT te.node_id) as test_count,
        SUM(CASE WHEN te.status = 'pass' THEN 1 ELSE 0 END) as passing_tests,
        SUM(CASE WHEN te.status IN ('fail', 'error') THEN 1 ELSE 0 END) as failing_tests
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS te
    WHERE te.generated_at >= DATEADD(day, -1, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    model_name,
    test_count,
    passing_tests,
    failing_tests,
    ROUND(passing_tests::FLOAT / NULLIF(test_count, 0) * 100, 1) as pass_rate_pct,
    CASE 
        WHEN test_count = 0 THEN '🔴 NO TESTS'
        WHEN test_count < 3 THEN '⚠️ LOW COVERAGE'
        WHEN pass_rate_pct < 100 THEN '⚠️ HAS FAILURES'
        ELSE '✅ GOOD'
    END as coverage_status
FROM model_test_counts
ORDER BY test_count ASC, pass_rate_pct ASC;
```

---

### **QUERY 9.4: Warehouse Utilization Efficiency**

```sql
-- ============================================================================
-- Warehouse Utilization - Cost Efficiency Analysis
-- ============================================================================
SELECT 
    warehouse_name,
    DATE_TRUNC('hour', start_time) as execution_hour,
    COUNT(*) as query_count,
    ROUND(SUM(execution_time) / 1000 / 3600, 4) as compute_hours,
    ROUND(AVG(execution_time) / 1000, 2) as avg_execution_seconds,
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 2) as total_gb_scanned,
    COUNT(DISTINCT user_name) as unique_users
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(day, -7, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 1, 2 DESC;
```

---

## 📋 SECTION 10: DASHBOARD DEPLOYMENT CHECKLIST

### **Setup Steps**

1. ✅ Install all dbt packages (`dbt deps`)
2. ✅ Run `setup_observability_dashboard.sql`
3. ✅ Run `setup_comprehensive_alerts.sql`
4. ✅ Run `setup_notifications.sql`
5. ✅ Configure email integration
6. ✅ Create Snowsight dashboard
7. ✅ Add all tile queries
8. ✅ Configure alerts and notifications
9. ✅ Enable scheduled tasks
10. ✅ Test alert notifications

### **Dashboard Layout Recommendation**

**Page 1: Executive Dashboard**
- TILE 7.1: Alert Summary Scorecard
- TILE 7.2: Critical Alerts Table
- TILE 8.3: Daily Metrics Summary
- TILE 7.6: Cost Tracking

**Page 2: Data Quality**
- TILE 7.3: Test Failure Trends
- TILE 7.11: Recurring Test Failures
- TILE 7.12: Test Pass Rate Degradation
- QUERY 9.3: Test Coverage Analysis

**Page 3: Performance**
- TILE 7.4: Performance Degradation
- TILE 7.7: Model Failure Analysis
- TILE 7.9: Long-Running Queries
- TILE 7.10: Warehouse Queuing

**Page 4: Operations**
- TILE 7.5: Data Freshness Heat Map
- TILE 7.14: Missing Data Loads
- TILE 8.1: Alert Audit Log
- TILE 8.2: Task Execution Status

**Page 5: Cost Optimization**
- TILE 7.13: Expensive Queries
- QUERY 9.4: Warehouse Utilization
- TILE 7.6: Cost Anomalies

---

## 🔔 Alert Configuration Guide

### **Critical Alerts (Immediate Email)**
- Critical test failures
- Model execution failures
- SLA violations
- Cost spikes > 3σ

### **High Priority (Email within 1 hour)**
- Performance degradation
- Stale data sources
- Warehouse queuing issues

### **Medium Priority (Daily Digest)**
- Test pass rate drops
- Expensive queries
- Missing data loads

### **Notification Channels**
1. Email: `data-team@company.com`
2. Slack: `#data-alerts` (via webhook)
3. Snowsight: Dashboard alerts
4. Audit Log: All alerts tracked

---

## 📊 Idempotency & Re-running

**All scripts are idempotent** - safe to run multiple times:
- ✅ `CREATE OR REPLACE VIEW` - Won't duplicate data
- ✅ `CREATE OR REPLACE PROCEDURE` - Updates definition
- ✅ `CREATE OR REPLACE TASK` - Resets schedule
- ✅ Data accumulates in `DBT_ARTIFACTS.*` tables automatically

**Running dbt build repeatedly:**
- Adds new execution records
- Does NOT duplicate
- Artifact tables grow with each run
- Views always show latest data

**Best Practice:**
- Run setup scripts once initially
- Re-run if you need to update definitions
- dbt build accumulates monitoring data naturally

---

**COMPREHENSIVE MONITORING COMPLETE!** 🎉

You now have:
- 14 Alert Views
- 20+ Dashboard Tiles
- Automated Notifications
- Full Observability Coverage
- Production-Ready Monitoring

Deploy to Snowsight and enjoy complete DBT observability! 🚀

