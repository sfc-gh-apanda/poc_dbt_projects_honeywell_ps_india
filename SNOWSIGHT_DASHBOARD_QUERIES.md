# Snowsight Dashboard Queries - Complete Observability

## 📊 Overview

This document provides all SQL queries needed to create a comprehensive DBT observability dashboard in Snowflake Snowsight using `dbt_artifacts` and `dbt-snowflake-monitoring` packages.

**Coverage:**
- ✅ DBT model execution tracking
- ✅ Test results history
- ✅ Source freshness monitoring
- ✅ Snowflake query costs
- ✅ Warehouse utilization
- ✅ Performance anomaly detection

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
        SUM(execution_time) as total_seconds,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_models,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed_models
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE DATE(generated_at) = CURRENT_DATE()
),
today_tests AS (
    SELECT 
        COUNT(*) as total_tests,
        SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed_tests,
        SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed_tests
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS
    WHERE DATE(generated_at) = CURRENT_DATE()
),
today_costs AS (
    SELECT 
        COALESCE(SUM(credits_used), 0) as total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
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
    DATE(generated_at) as execution_date,
    COUNT(DISTINCT node_id) as models_run,
    ROUND(SUM(execution_time) / 60, 1) as total_minutes,
    ROUND(AVG(execution_time), 2) as avg_seconds,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed,
    ROUND(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate_pct
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE generated_at >= DATEADD(day, -30, CURRENT_DATE())
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
    ROUND(AVG(execution_time), 2) as avg_seconds,
    ROUND(MAX(execution_time), 2) as max_seconds,
    ROUND(MIN(execution_time), 2) as min_seconds,
    ROUND(STDDEV(execution_time), 2) as stddev_seconds,
    CASE 
        WHEN AVG(execution_time) > 300 THEN '🔴 CRITICAL'
        WHEN AVG(execution_time) > 60 THEN '🟡 SLOW'
        WHEN AVG(execution_time) > 10 THEN '🟢 MODERATE'
        ELSE '⚪ FAST'
    END as performance_tier
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
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
    DATE(generated_at) as test_date,
    status,
    COUNT(*) as test_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(generated_at)), 2) as percentage
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE generated_at >= DATEADD(day, -30, CURRENT_DATE())
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
SELECT 
    DATE(start_time) as query_date,
    COUNT(*) as query_count,
    ROUND(SUM(execution_time) / 1000 / 60, 1) as total_minutes,
    ROUND(SUM(credits_used), 3) as total_credits,
    ROUND(SUM(credits_used) * 3.0, 2) as estimated_cost_usd,  -- Adjust rate as needed
    ROUND(AVG(credits_used), 4) as avg_credits_per_query,
    ROUND(MAX(credits_used), 3) as max_credits_single_query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY query_date
ORDER BY query_date DESC;
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
-- Cost by Model (Last 7 Days)
-- ============================================================================
WITH dbt_queries AS (
    SELECT 
        query_text,
        execution_time,
        credits_used,
        start_time,
        -- Extract model name from query
        REGEXP_SUBSTR(query_text, 'create.*?(table|view)\\s+([\\w.]+)', 1, 1, 'ie', 2) as model_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATEADD(day, -7, CURRENT_DATE())
      AND credits_used > 0
)
SELECT 
    COALESCE(model_name, 'Unknown') as model_name,
    COUNT(*) as run_count,
    ROUND(SUM(credits_used), 3) as total_credits,
    ROUND(SUM(credits_used) * 3.0, 2) as estimated_cost_usd,
    ROUND(AVG(execution_time / 1000), 2) as avg_seconds,
    ROUND(SUM(credits_used) / COUNT(*), 4) as avg_credits_per_run
FROM dbt_queries
WHERE model_name IS NOT NULL
GROUP BY model_name
ORDER BY total_credits DESC
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
    generated_at as failure_time,
    SPLIT_PART(node_id, '.', -1) as test_name,
    status,
    failures as failed_row_count,
    SUBSTRING(message, 1, 100) as error_message,
    ROUND(execution_time, 2) as execution_seconds
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND generated_at >= DATEADD(hour, -24, CURRENT_DATE())
ORDER BY generated_at DESC;
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
        execution_time,
        CASE 
            WHEN execution_time < 1 THEN '< 1s'
            WHEN execution_time < 5 THEN '1-5s'
            WHEN execution_time < 10 THEN '5-10s'
            WHEN execution_time < 30 THEN '10-30s'
            WHEN execution_time < 60 THEN '30-60s'
            WHEN execution_time < 300 THEN '1-5min'
            ELSE '> 5min'
        END as time_bucket,
        CASE 
            WHEN execution_time < 1 THEN 1
            WHEN execution_time < 5 THEN 2
            WHEN execution_time < 10 THEN 3
            WHEN execution_time < 30 THEN 4
            WHEN execution_time < 60 THEN 5
            WHEN execution_time < 300 THEN 6
            ELSE 7
        END as bucket_order
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
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
-- Warehouse Credit Usage by DBT (Last 7 Days)
-- ============================================================================
SELECT 
    DATE(start_time) as usage_date,
    warehouse_name,
    ROUND(SUM(credits_used), 3) as total_credits,
    ROUND(SUM(credits_used) * 3.0, 2) as estimated_cost_usd,
    COUNT(*) as query_count,
    ROUND(AVG(execution_time / 1000), 2) as avg_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(day, -7, CURRENT_DATE())
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
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
QUALIFY ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY generated_at DESC) = 1
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
        AVG(execution_time) as baseline_avg,
        STDDEV(execution_time) as baseline_stddev
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'success'
    GROUP BY node_id
),
recent AS (
    -- Recent: Last 24 hours
    SELECT 
        node_id,
        AVG(execution_time) as recent_avg
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -1, CURRENT_DATE())
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
    WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
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
-- Models to Optimize (High Cost + Slow Execution)
-- ============================================================================
WITH model_stats AS (
    SELECT 
        SPLIT_PART(node_id, '.', -1) as model_name,
        COUNT(*) as run_count,
        AVG(execution_time) as avg_execution_time
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
      AND status = 'success'
    GROUP BY node_id
),
model_costs AS (
    SELECT 
        REGEXP_SUBSTR(query_text, 'create.*?table\\s+([\\w.]+)', 1, 1, 'ie', 1) as model_name,
        SUM(credits_used) as total_credits,
        AVG(execution_time / 1000) as avg_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY model_name
)
SELECT 
    s.model_name,
    s.run_count,
    ROUND(s.avg_execution_time, 2) as avg_seconds,
    ROUND(COALESCE(c.total_credits, 0), 3) as total_credits,
    ROUND(COALESCE(c.total_credits, 0) * 3.0, 2) as estimated_cost_usd,
    CASE 
        WHEN s.avg_execution_time > 60 AND c.total_credits > 1 THEN '🔴 High Priority'
        WHEN s.avg_execution_time > 30 AND c.total_credits > 0.5 THEN '🟡 Medium Priority'
        ELSE '🟢 Low Priority'
    END as optimization_priority
FROM model_stats s
LEFT JOIN model_costs c ON s.model_name = c.model_name
WHERE s.avg_execution_time > 10 OR c.total_credits > 0.1
ORDER BY c.total_credits DESC, s.avg_execution_time DESC
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
        ROUND(SUM(execution_time) / 60, 1) as total_minutes,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failures
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATE_TRUNC('week', CURRENT_DATE())
),
last_week AS (
    SELECT 
        COUNT(DISTINCT node_id) as models,
        ROUND(SUM(execution_time) / 60, 1) as total_minutes,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failures
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(week, -1, DATE_TRUNC('week', CURRENT_DATE()))
      AND generated_at < DATE_TRUNC('week', CURRENT_DATE())
),
this_week_cost AS (
    SELECT ROUND(SUM(credits_used), 2) as credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATE_TRUNC('week', CURRENT_DATE())
),
last_week_cost AS (
    SELECT ROUND(SUM(credits_used), 2) as credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATEADD(week, -1, DATE_TRUNC('week', CURRENT_DATE()))
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

