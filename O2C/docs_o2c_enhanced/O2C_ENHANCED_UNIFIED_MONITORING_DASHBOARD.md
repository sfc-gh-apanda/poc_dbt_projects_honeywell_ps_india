# O2C Enhanced - Unified Monitoring Dashboard

**Purpose:** Single comprehensive monitoring dashboard covering ALL observability metrics  
**Platform:** Snowsight / Tableau / Power BI  
**Created:** January 2025  
**Status:** ✅ Production Ready  
**Prerequisites:** All monitoring views from setup scripts must be deployed

---

## 📋 Table of Contents

1. [Executive Health Scorecard](#-executive-health-scorecard)
2. [Run Metrics & Execution Tracking](#-run-metrics--execution-tracking)
3. [Model Performance Metrics](#-model-performance-metrics)
4. [Error Analysis & Trends](#-error-analysis--trends)
5. [Data Quality & Test Metrics](#-data-quality--test-metrics)
6. [Data Observability](#-data-observability)
7. [Cost & Resource Optimization](#-cost--resource-optimization)
8. [Infrastructure Health](#-infrastructure-health)
9. [Alert Management](#-alert-management)
10. [Dashboard Setup Guide](#-dashboard-setup-guide)

---

## 🎯 Executive Health Scorecard

### **TILE 1: Platform Health Overview**

**Purpose:** Single source of truth for platform health  
**Type:** Scorecard (12 metrics)  
**Refresh:** Every 5 minutes  
**Audience:** Everyone

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Platform Health Overview - Complete Status at a Glance
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    -- Overall Health
    h.health_status AS platform_status,
    h.health_score AS platform_score,
    
    -- Business Metrics
    k.total_orders,
    ROUND(k.total_order_value / 1000, 1) AS order_value_k,
    ROUND(k.total_ar_outstanding / 1000, 1) AS ar_outstanding_k,
    k.avg_dso,
    
    -- Operational Health
    o.builds_24h,
    o.build_success_rate_24h,
    o.test_pass_rate_24h,
    o.data_completeness_score,
    
    -- Alert Status
    h.total_critical_alerts,
    (SELECT COUNT(*) FROM EDW.O2C_AUDIT.V_ACTIVE_ALERTS WHERE severity = 'CRITICAL') AS active_critical_alerts,
    
    -- Last Updated
    h.snapshot_time
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY h
CROSS JOIN EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUSINESS_KPIS k
CROSS JOIN EDW.O2C_ENHANCED_MONITORING.O2C_ENH_OPERATIONAL_SUMMARY o;
```

**Expected Output:**
| Metric | Value | Status |
|--------|-------|--------|
| Platform Status | 🟢 HEALTHY | Good |
| Platform Score | 95 | Excellent |
| Total Orders | 1,234 | - |
| Order Value | $1,234K | - |
| AR Outstanding | $567K | - |
| Avg DSO | 45 days | - |
| Builds 24h | 12 | - |
| Build Success Rate | 100% | Good |
| Test Pass Rate | 98% | Good |
| Data Completeness | 99% | Excellent |
| Total Critical Alerts | 0 | Good |
| Active Critical | 0 | Good |

---

## 🔄 Run Metrics & Execution Tracking

### **TILE 2: Daily Run Summary (Last 30 Days)**

**Purpose:** Track daily execution patterns and success rates  
**Type:** Line chart with bars  
**Refresh:** Hourly

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Daily Run Summary with Trend Analysis
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    execution_date,
    
    -- Volume Metrics
    models_run,
    successful_models,
    failed_models,
    
    -- Performance Metrics
    total_minutes,
    avg_execution_seconds,
    max_execution_seconds,
    
    -- Success Rate
    success_rate_pct,
    
    -- Trend Indicator
    CASE 
        WHEN success_rate_pct >= 95 THEN '🟢 EXCELLENT'
        WHEN success_rate_pct >= 90 THEN '🟡 GOOD'
        WHEN success_rate_pct >= 80 THEN '🟠 WARNING'
        ELSE '🔴 CRITICAL'
    END AS health_status,
    
    -- 7-day moving averages
    AVG(success_rate_pct) OVER (
        ORDER BY execution_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS success_rate_7d_avg,
    
    AVG(total_minutes) OVER (
        ORDER BY execution_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS duration_7d_avg
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DAILY_EXECUTION_SUMMARY
WHERE execution_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY execution_date DESC;
```

### **TILE 3: Run-Level Details (Last 7 Days)**

**Purpose:** Detailed run history with duration and status  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Run-Level Details with Complete Audit Trail
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    run_id,
    project_name,
    environment,
    
    -- Timing
    run_started_at,
    run_ended_at,
    ROUND(run_duration_seconds / 60, 2) AS duration_minutes,
    
    -- Results
    run_status,
    models_run,
    models_success,
    models_failed,
    success_rate_pct,
    
    -- Classification
    duration_category,
    run_health,
    
    -- Resource Info
    warehouse_name,
    user_name,
    
    -- Time Since Run
    DATEDIFF('hour', run_started_at, CURRENT_TIMESTAMP()) AS hours_ago
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_RUN_HISTORY
WHERE run_started_at >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY run_started_at DESC
LIMIT 50;
```

### **TILE 4: Run Execution Timeline (Gantt View)**

**Purpose:** Visual timeline of model execution within runs  
**Type:** Timeline/Gantt  
**Refresh:** Real-time

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Execution Timeline - See What's Running and When
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    run_id,
    model_name,
    schema_name,
    
    -- Timing
    started_at,
    completed_at,
    execution_seconds,
    
    -- Status
    status,
    CASE 
        WHEN status = 'SUCCESS' THEN '🟢'
        WHEN status = 'FAIL' THEN '🔴'
        WHEN status = 'RUNNING' THEN '🔵'
        ELSE '⚪'
    END AS status_icon,
    
    -- Parallel Execution Info
    COUNT(*) OVER (
        PARTITION BY run_id 
        ORDER BY started_at 
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS models_remaining,
    
    -- Sequence
    ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY started_at) AS execution_order
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_EXECUTION_TIMELINE
WHERE started_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY started_at DESC, execution_order;
```

---

## 📊 Model Performance Metrics

### **TILE 5: Model Performance Dashboard**

**Purpose:** Complete model performance analysis  
**Type:** Multi-metric table  
**Refresh:** Every 4 hours

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Model Performance Comprehensive Dashboard
-- ═══════════════════════════════════════════════════════════════════════════════
WITH latest_runs AS (
    SELECT 
        model_name,
        schema_name,
        
        -- Execution Metrics
        run_count,
        avg_seconds,
        max_seconds,
        min_seconds,
        total_seconds,
        
        -- Performance Tier
        performance_tier,
        
        -- Cost
        estimated_cost_usd,
        cost_per_execution,
        
        -- Rank
        ROW_NUMBER() OVER (ORDER BY avg_seconds DESC) AS slowness_rank
        
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
),
performance_trends AS (
    SELECT 
        model_name,
        schema_name,
        avg_7day_ma,
        baseline_avg,
        variance_from_baseline_pct,
        performance_trend,
        ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY run_date DESC) AS rn
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_PERFORMANCE_TREND
),
efficiency_metrics AS (
    SELECT 
        model_name,
        schema_name,
        load_strategy,
        rows_per_second,
        seconds_per_1k_rows,
        efficiency_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
)
SELECT 
    l.model_name,
    l.schema_name,
    
    -- Execution Stats
    l.run_count,
    l.avg_seconds,
    l.max_seconds,
    l.performance_tier,
    
    -- Trend Analysis
    t.performance_trend,
    t.variance_from_baseline_pct,
    
    -- Efficiency
    e.load_strategy,
    e.rows_per_second,
    e.efficiency_status,
    
    -- Cost
    l.estimated_cost_usd,
    l.cost_per_execution,
    
    -- Ranking
    l.slowness_rank,
    
    -- Overall Health Score
    CASE 
        WHEN t.performance_trend LIKE '%DEGRADED%' THEN '🔴 DEGRADING'
        WHEN l.performance_tier = '🔴 CRITICAL' THEN '🔴 SLOW'
        WHEN e.efficiency_status LIKE '%INEFFICIENT%' THEN '🟠 INEFFICIENT'
        WHEN l.performance_tier = '🟢 FAST' THEN '🟢 HEALTHY'
        ELSE '🟡 NORMAL'
    END AS overall_health
    
FROM latest_runs l
LEFT JOIN performance_trends t ON l.model_name = t.model_name AND t.rn = 1
LEFT JOIN efficiency_metrics e ON l.model_name = e.model_name
ORDER BY 
    CASE overall_health
        WHEN '🔴 DEGRADING' THEN 1
        WHEN '🔴 SLOW' THEN 2
        WHEN '🟠 INEFFICIENT' THEN 3
        ELSE 4
    END,
    l.avg_seconds DESC
LIMIT 50;
```

### **TILE 6: Compilation Analysis**

**Purpose:** Track query compilation overhead  
**Type:** Line chart  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Compilation Performance Analysis
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    query_date,
    
    -- Volume
    query_count,
    
    -- Compilation Metrics
    avg_compilation_seconds,
    median_compilation_seconds,
    p95_compilation_seconds,
    p99_compilation_seconds,
    
    -- Problem Queries
    slow_compile_count,
    slow_compile_pct,
    
    -- Total Time Lost
    total_compilation_minutes,
    
    -- Health Status
    compile_health,
    CASE 
        WHEN slow_compile_pct > 10 THEN '🔴 HIGH COMPILE OVERHEAD'
        WHEN slow_compile_pct > 5 THEN '🟠 MODERATE OVERHEAD'
        ELSE '🟢 HEALTHY'
    END AS compile_status,
    
    -- Trend
    AVG(avg_compilation_seconds) OVER (
        ORDER BY query_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS compile_7d_avg
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COMPILATION_ANALYSIS
WHERE query_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY query_date DESC;
```

### **TILE 7: Build Performance Metrics**

**Purpose:** Track build-time metrics and patterns  
**Type:** Table with trends  
**Refresh:** Every 4 hours

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Build Performance - Full Execution Cycle Metrics
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    DATE(run_started_at) AS build_date,
    
    -- Build Metrics
    COUNT(DISTINCT run_id) AS total_builds,
    SUM(models_run) AS total_models_built,
    
    -- Timing
    AVG(run_duration_seconds / 60) AS avg_build_minutes,
    MIN(run_duration_seconds / 60) AS min_build_minutes,
    MAX(run_duration_seconds / 60) AS max_build_minutes,
    
    -- Success Rate
    SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_builds,
    SUM(CASE WHEN run_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_builds,
    ROUND(
        SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        1
    ) AS build_success_rate_pct,
    
    -- Performance Classification
    CASE 
        WHEN AVG(run_duration_seconds / 60) < 5 THEN '🟢 FAST (<5 min)'
        WHEN AVG(run_duration_seconds / 60) < 15 THEN '🟡 NORMAL (5-15 min)'
        WHEN AVG(run_duration_seconds / 60) < 30 THEN '🟠 SLOW (15-30 min)'
        ELSE '🔴 VERY SLOW (>30 min)'
    END AS build_speed,
    
    -- Health Status
    CASE 
        WHEN ROUND(SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) >= 95 
        THEN '🟢 HEALTHY'
        WHEN ROUND(SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) >= 90 
        THEN '🟡 GOOD'
        ELSE '🔴 NEEDS ATTENTION'
    END AS build_health
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_RUN_HISTORY
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY build_date
ORDER BY build_date DESC;
```

---

## 🚨 Error Analysis & Trends

### **TILE 8: Error Dashboard - Complete View**

**Purpose:** Comprehensive error tracking and categorization  
**Type:** Multi-section table  
**Refresh:** Every 15 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Error Analysis Dashboard - All Error Metrics
-- ═══════════════════════════════════════════════════════════════════════════════
WITH error_summary AS (
    SELECT 
        DATE(error_time) AS error_date,
        schema_name,
        error_category,
        error_code,
        COUNT(*) AS error_count,
        COUNT(DISTINCT LEFT(error_message, 100)) AS unique_error_types,
        MAX(error_time) AS last_occurrence
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
    WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY error_date, schema_name, error_category, error_code
),
error_trends AS (
    SELECT 
        date,
        error_count,
        success_count,
        error_rate_pct,
        error_rate_7day_avg
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_TREND
    WHERE date >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT 
    -- Date & Source
    es.error_date,
    es.schema_name,
    
    -- Error Classification
    es.error_category,
    es.error_code,
    
    -- Frequency
    es.error_count,
    es.unique_error_types,
    
    -- Trend
    et.error_rate_pct AS daily_error_rate,
    et.error_rate_7day_avg AS trend_error_rate,
    
    -- Recency
    es.last_occurrence,
    DATEDIFF('hour', es.last_occurrence, CURRENT_TIMESTAMP()) AS hours_since_last,
    
    -- Severity
    CASE 
        WHEN es.error_count >= 100 THEN '🔴 CRITICAL (100+ errors)'
        WHEN es.error_count >= 50 THEN '🟠 HIGH (50+ errors)'
        WHEN es.error_count >= 10 THEN '🟡 MEDIUM (10+ errors)'
        ELSE '🟢 LOW (<10 errors)'
    END AS severity,
    
    -- Pattern
    CASE 
        WHEN es.unique_error_types = 1 THEN '🔄 REPEATING (Same error)'
        WHEN es.unique_error_types < es.error_count / 2 THEN '🔁 CLUSTERED (Few types)'
        ELSE '📊 DIVERSE (Many types)'
    END AS error_pattern
    
FROM error_summary es
LEFT JOIN error_trends et ON es.error_date = et.date
ORDER BY 
    es.error_date DESC,
    es.error_count DESC
LIMIT 100;
```

### **TILE 9: Error Trend Analysis (30 Days)**

**Purpose:** Track error rates over time with anomaly detection  
**Type:** Line chart with threshold  
**Refresh:** Hourly

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Error Trend with Anomaly Detection
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    date,
    
    -- Volume
    total_queries,
    error_count,
    success_count,
    
    -- Rates
    error_rate_pct,
    success_rate_pct,
    error_rate_7day_avg,
    
    -- Anomaly Detection
    CASE 
        WHEN error_rate_pct > error_rate_7day_avg * 2 THEN '🔴 ANOMALY (2x avg)'
        WHEN error_rate_pct > error_rate_7day_avg * 1.5 THEN '🟠 SPIKE (1.5x avg)'
        WHEN error_rate_pct > 5 THEN '🟡 ELEVATED (>5%)'
        ELSE '🟢 NORMAL'
    END AS trend_status,
    
    -- Standard Deviation
    STDDEV(error_rate_pct) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS error_rate_stddev
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_TREND
WHERE date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY date DESC;
```

### **TILE 10: Model Failure Analysis**

**Purpose:** Track failing models and patterns  
**Type:** Table with severity  
**Refresh:** Every 15 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Model Failure Analysis with Root Cause Indicators
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    model_name,
    schema_name,
    
    -- Failure Metrics
    failures_last_7_days,
    total_runs_last_7_days,
    ROUND(failures_last_7_days * 100.0 / NULLIF(total_runs_last_7_days, 0), 1) AS failure_rate_pct,
    
    -- Timing
    failure_time,
    DATEDIFF('hour', failure_time, CURRENT_TIMESTAMP()) AS hours_since_failure,
    
    -- Execution Info
    warehouse_name,
    execution_seconds,
    
    -- Severity
    severity,
    CASE 
        WHEN failures_last_7_days >= 10 THEN '🔴 CHRONIC (10+ failures)'
        WHEN failures_last_7_days >= 5 THEN '🟠 RECURRING (5+ failures)'
        WHEN failures_last_7_days >= 2 THEN '🟡 MULTIPLE (2+ failures)'
        ELSE '🟢 ISOLATED (1 failure)'
    END AS failure_pattern
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_MODEL_FAILURES
WHERE failure_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    failures_last_7_days DESC,
    failure_time DESC
LIMIT 50;
```

### **TILE 11: Build Failure Details with Root Cause**

**Purpose:** Detailed build failure analysis with error categorization  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Build Failure Details with Classification
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    failure_time,
    affected_object,
    
    -- Error Classification
    error_category,
    error_code,
    LEFT(error_message, 200) AS error_preview,
    
    -- Context
    execution_seconds,
    warehouse_name,
    user_name,
    
    -- Severity
    recency_severity,
    CASE 
        WHEN error_category = 'SYNTAX_ERROR' THEN '📝 Code Issue'
        WHEN error_category = 'PERMISSION_ERROR' THEN '🔒 Access Issue'
        WHEN error_category = 'RESOURCE_ERROR' THEN '⚡ Resource Issue'
        WHEN error_category = 'DATA_ERROR' THEN '📊 Data Issue'
        WHEN error_category = 'TIMEOUT' THEN '⏱️ Timeout Issue'
        ELSE '❓ Other'
    END AS root_cause_category,
    
    -- Recency
    DATEDIFF('hour', failure_time, CURRENT_TIMESTAMP()) AS hours_ago
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUILD_FAILURE_DETAILS
WHERE failure_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY failure_time DESC
LIMIT 50;
```

---

## ✅ Data Quality & Test Metrics

### **TILE 12: Test Execution Dashboard**

**Purpose:** Complete test metrics and coverage  
**Type:** Multi-metric scorecard  
**Refresh:** Every 4 hours

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Test Execution Dashboard - Complete Test Metrics
-- ═══════════════════════════════════════════════════════════════════════════════
WITH test_summary AS (
    SELECT 
        test_type,
        total_executions,
        passed,
        failed,
        pass_rate_pct,
        avg_execution_sec,
        health_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_SUMMARY_BY_TYPE
),
test_coverage AS (
    SELECT 
        COUNT(*) AS total_models,
        SUM(CASE WHEN test_count = 0 THEN 1 ELSE 0 END) AS models_without_tests,
        SUM(CASE WHEN test_count > 0 THEN 1 ELSE 0 END) AS models_with_tests,
        ROUND(
            SUM(CASE WHEN test_count > 0 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(COUNT(*), 0), 
            1
        ) AS coverage_pct
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE
),
recent_trend AS (
    SELECT 
        pass_rate_pct AS latest_pass_rate,
        pass_rate_7day_avg
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_PASS_RATE_TREND
    ORDER BY test_date DESC
    LIMIT 1
)
SELECT 
    -- Coverage Metrics
    tc.total_models,
    tc.models_with_tests,
    tc.models_without_tests,
    tc.coverage_pct,
    
    -- Recent Execution
    rt.latest_pass_rate,
    rt.pass_rate_7day_avg,
    
    -- By Test Type
    (SELECT SUM(total_executions) FROM test_summary) AS total_test_executions,
    (SELECT SUM(passed) FROM test_summary) AS total_passed,
    (SELECT SUM(failed) FROM test_summary) AS total_failed,
    
    -- Health Status
    CASE 
        WHEN tc.coverage_pct >= 95 AND rt.latest_pass_rate >= 95 THEN '🟢 EXCELLENT'
        WHEN tc.coverage_pct >= 80 AND rt.latest_pass_rate >= 90 THEN '🟡 GOOD'
        WHEN tc.coverage_pct >= 60 OR rt.latest_pass_rate >= 80 THEN '🟠 NEEDS IMPROVEMENT'
        ELSE '🔴 CRITICAL'
    END AS overall_test_health,
    
    CURRENT_TIMESTAMP() AS snapshot_time
FROM test_coverage tc
CROSS JOIN recent_trend rt;
```

### **TILE 13: Test Pass Rate Trend (30 Days)**

**Purpose:** Track test pass rates over time  
**Type:** Line chart with moving average  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Test Pass Rate Trend with Quality Gates
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    test_date,
    
    -- Test Counts
    total_tests,
    passed_tests,
    failed_tests,
    
    -- Rates
    pass_rate_pct,
    pass_rate_7day_avg,
    
    -- Quality Gate Status
    CASE 
        WHEN pass_rate_pct >= 98 THEN '🟢 EXCELLENT (≥98%)'
        WHEN pass_rate_pct >= 95 THEN '🟡 GOOD (≥95%)'
        WHEN pass_rate_pct >= 90 THEN '🟠 ACCEPTABLE (≥90%)'
        ELSE '🔴 BELOW STANDARD (<90%)'
    END AS quality_gate_status,
    
    -- Trend
    CASE 
        WHEN pass_rate_pct > pass_rate_7day_avg + 2 THEN '📈 IMPROVING'
        WHEN pass_rate_pct < pass_rate_7day_avg - 2 THEN '📉 DECLINING'
        ELSE '➡️ STABLE'
    END AS trend_direction
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_PASS_RATE_TREND
WHERE test_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY test_date DESC;
```

### **TILE 14: Test Coverage by Model**

**Purpose:** Identify models lacking test coverage  
**Type:** Table with coverage indicators  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Test Coverage Analysis - Models Needing Tests
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    model_name,
    schema_name,
    
    -- Test Metrics
    test_count,
    passed_tests,
    failed_tests,
    pass_rate_pct,
    
    -- Coverage Status
    coverage_status,
    test_health,
    
    -- Prioritization
    CASE 
        WHEN test_count = 0 AND schema_name LIKE '%CORE%' THEN '🔴 P0 - Core model without tests'
        WHEN test_count = 0 AND schema_name LIKE '%DIMENSION%' THEN '🔴 P0 - Dimension without tests'
        WHEN test_count = 0 THEN '🟠 P1 - No test coverage'
        WHEN test_count = 1 THEN '🟡 P2 - Low coverage (1 test)'
        WHEN failed_tests > 0 THEN '🔴 P0 - Failing tests'
        ELSE '🟢 OK'
    END AS priority,
    
    -- Last Run
    last_model_run,
    DATEDIFF('day', last_model_run, CURRENT_DATE()) AS days_since_run
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE
ORDER BY 
    CASE 
        WHEN test_count = 0 AND schema_name LIKE '%CORE%' THEN 1
        WHEN failed_tests > 0 THEN 2
        WHEN test_count = 0 THEN 3
        ELSE 4
    END,
    model_name;
```

### **TILE 15: Recurring Test Failures**

**Purpose:** Identify persistent test failures  
**Type:** Table with severity  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Recurring Test Failures - Persistent Issues
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    test_type,
    test_identifier,
    
    -- Failure Pattern
    days_with_failures,
    total_failures,
    
    -- Timeline
    first_failure,
    last_failure,
    DATEDIFF('day', first_failure, last_failure) AS failure_span_days,
    
    -- Severity
    severity,
    alert_description,
    
    -- Impact
    CASE 
        WHEN days_with_failures >= 7 THEN '🔴 CHRONIC (7+ days)'
        WHEN days_with_failures >= 3 THEN '🟠 RECURRING (3+ days)'
        ELSE '🟡 INTERMITTENT'
    END AS failure_pattern,
    
    -- Priority
    CASE 
        WHEN test_type IN ('not_null_test', 'unique_test') AND days_with_failures >= 3 
        THEN '🔴 HIGH PRIORITY'
        WHEN days_with_failures >= 7 
        THEN '🔴 HIGH PRIORITY'
        WHEN total_failures >= 10 
        THEN '🟠 MEDIUM PRIORITY'
        ELSE '🟡 LOW PRIORITY'
    END AS action_priority
    
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

## 🔍 Data Observability

### **TILE 16: Data Freshness Dashboard**

**Purpose:** Monitor data freshness across all layers  
**Type:** Table with status indicators  
**Refresh:** Every 30 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Data Freshness - Source and Model Layer Monitoring
-- ═══════════════════════════════════════════════════════════════════════════════
WITH source_freshness AS (
    SELECT 
        'SOURCE' AS layer,
        source_table AS object_name,
        source_type AS object_type,
        row_count,
        last_load_timestamp AS last_update,
        hours_since_load AS staleness_hours,
        freshness_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SOURCE_FRESHNESS
),
model_freshness AS (
    SELECT 
        'MODEL' AS layer,
        model_name AS object_name,
        layer AS object_type,
        row_count,
        last_refresh AS last_update,
        minutes_since_refresh / 60.0 AS staleness_hours,
        freshness_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_FRESHNESS
)
SELECT 
    layer,
    object_name,
    object_type,
    row_count,
    last_update,
    staleness_hours,
    freshness_status,
    
    -- SLA Compliance
    CASE 
        WHEN layer = 'SOURCE' AND staleness_hours <= 24 THEN '✅ Within SLA (<24h)'
        WHEN layer = 'MODEL' AND staleness_hours <= 1 THEN '✅ Within SLA (<1h)'
        WHEN layer = 'SOURCE' AND staleness_hours <= 48 THEN '⚠️ Near SLA (24-48h)'
        WHEN layer = 'MODEL' AND staleness_hours <= 4 THEN '⚠️ Near SLA (1-4h)'
        ELSE '❌ SLA Breach'
    END AS sla_status,
    
    -- Priority
    CASE 
        WHEN freshness_status LIKE '%CRITICAL%' THEN '🔴 P0'
        WHEN freshness_status LIKE '%STALE%' THEN '🟠 P1'
        WHEN freshness_status LIKE '%WARNING%' THEN '🟡 P2'
        ELSE '🟢 OK'
    END AS priority
    
FROM (
    SELECT * FROM source_freshness
    UNION ALL
    SELECT * FROM model_freshness
) combined
ORDER BY 
    CASE priority
        WHEN '🔴 P0' THEN 1
        WHEN '🟠 P1' THEN 2
        WHEN '🟡 P2' THEN 3
        ELSE 4
    END,
    staleness_hours DESC;
```

### **TILE 17: Data Flow Reconciliation**

**Purpose:** Validate data flows correctly through layers  
**Type:** Table with variance detection  
**Refresh:** Hourly

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Data Flow Reconciliation - Source to Mart Validation
-- ═══════════════════════════════════════════════════════════════════════════════
WITH all_layers AS (
    SELECT 
        layer,
        table_name,
        description,
        row_count,
        latest_record,
        checked_at
    FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING
),
layered_comparison AS (
    SELECT 
        description AS entity,
        MAX(CASE WHEN layer = 'SOURCE' THEN row_count END) AS source_rows,
        MAX(CASE WHEN layer = 'STAGING' THEN row_count END) AS staging_rows,
        MAX(CASE WHEN layer = 'CORE' THEN row_count END) AS core_rows,
        MAX(CASE WHEN layer = 'SOURCE' THEN latest_record END) AS source_latest,
        MAX(CASE WHEN layer = 'CORE' THEN latest_record END) AS core_latest
    FROM all_layers
    WHERE description IN ('Orders', 'Invoices', 'Payments')
    GROUP BY entity
)
SELECT 
    entity,
    
    -- Row Counts by Layer
    source_rows,
    staging_rows,
    core_rows,
    
    -- Variances
    staging_rows - source_rows AS source_to_staging_variance,
    ROUND((staging_rows - source_rows) * 100.0 / NULLIF(source_rows, 0), 2) AS source_to_staging_pct,
    
    -- Validation Status
    CASE 
        WHEN source_rows = staging_rows THEN '✅ EXACT MATCH'
        WHEN ABS((staging_rows - source_rows) * 100.0 / NULLIF(source_rows, 0)) < 0.1 THEN '🟢 ACCEPTABLE (<0.1%)'
        WHEN ABS((staging_rows - source_rows) * 100.0 / NULLIF(source_rows, 0)) < 1 THEN '🟡 MINOR VARIANCE (<1%)'
        WHEN ABS((staging_rows - source_rows) * 100.0 / NULLIF(source_rows, 0)) < 5 THEN '🟠 MODERATE VARIANCE (<5%)'
        ELSE '🔴 SIGNIFICANT VARIANCE (≥5%)'
    END AS validation_status,
    
    -- Data Latency
    CASE 
        WHEN core_latest IS NOT NULL AND source_latest IS NOT NULL 
        THEN DATEDIFF('hour', source_latest, core_latest)
        ELSE NULL
    END AS data_latency_hours,
    
    -- Completeness
    ROUND(COALESCE(core_rows, 0) * 100.0 / NULLIF(source_rows, 0), 1) AS data_completeness_pct,
    
    CURRENT_TIMESTAMP() AS validated_at
    
FROM layered_comparison
ORDER BY entity;
```

### **TILE 18: Data Quality Scorecard**

**Purpose:** Overall data quality metrics  
**Type:** Scorecard  
**Refresh:** Hourly

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Data Quality Scorecard - Complete DQ Metrics
-- ═══════════════════════════════════════════════════════════════════════════════
WITH completeness AS (
    SELECT 
        dataset,
        total_orders,
        invoice_rate_pct,
        payment_rate_pct,
        customer_enrichment_pct,
        completeness_score,
        quality_grade
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DATA_COMPLETENESS
),
reconciliation AS (
    SELECT 
        SUM(CASE WHEN validation_status LIKE '%MATCH%' THEN 1 ELSE 0 END) AS matched_entities,
        COUNT(*) AS total_entities,
        ROUND(
            SUM(CASE WHEN validation_status LIKE '%MATCH%' THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(COUNT(*), 0), 
            1
        ) AS reconciliation_score
    FROM EDW.O2C_AUDIT.V_DATA_FLOW_VALIDATION
),
null_analysis AS (
    SELECT 
        SUM(CASE WHEN quality_status LIKE '%CRITICAL%' THEN 1 ELSE 0 END) AS critical_null_issues,
        SUM(CASE WHEN quality_status LIKE '%HIGH%' THEN 1 ELSE 0 END) AS high_null_issues,
        COUNT(*) AS total_columns_checked
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_NULL_RATE_ANALYSIS
),
pk_validation AS (
    SELECT 
        SUM(CASE WHEN pk_status = '✅ VALID' THEN 1 ELSE 0 END) AS valid_pks,
        COUNT(*) AS total_tables_checked
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION
)
SELECT 
    -- Completeness
    c.completeness_score AS completeness_pct,
    c.quality_grade AS completeness_grade,
    
    -- Reconciliation
    r.reconciliation_score AS reconciliation_pct,
    
    -- Null Quality
    n.critical_null_issues,
    n.high_null_issues,
    ROUND((n.total_columns_checked - n.critical_null_issues - n.high_null_issues) * 100.0 / 
          NULLIF(n.total_columns_checked, 0), 1) AS null_quality_pct,
    
    -- Primary Key Integrity
    pk.valid_pks,
    pk.total_tables_checked,
    ROUND(pk.valid_pks * 100.0 / NULLIF(pk.total_tables_checked, 0), 1) AS pk_validity_pct,
    
    -- Overall DQ Score (weighted average)
    ROUND(
        (c.completeness_score * 0.3 + 
         r.reconciliation_score * 0.3 + 
         ((n.total_columns_checked - n.critical_null_issues - n.high_null_issues) * 100.0 / 
          NULLIF(n.total_columns_checked, 0)) * 0.2 +
         (pk.valid_pks * 100.0 / NULLIF(pk.total_tables_checked, 0)) * 0.2),
        1
    ) AS overall_dq_score,
    
    -- Overall Grade
    CASE 
        WHEN ROUND(
            (c.completeness_score * 0.3 + 
             r.reconciliation_score * 0.3 + 
             ((n.total_columns_checked - n.critical_null_issues - n.high_null_issues) * 100.0 / 
              NULLIF(n.total_columns_checked, 0)) * 0.2 +
             (pk.valid_pks * 100.0 / NULLIF(pk.total_tables_checked, 0)) * 0.2),
            1
        ) >= 95 THEN '🟢 A - EXCELLENT'
        WHEN ROUND(
            (c.completeness_score * 0.3 + 
             r.reconciliation_score * 0.3 + 
             ((n.total_columns_checked - n.critical_null_issues - n.high_null_issues) * 100.0 / 
              NULLIF(n.total_columns_checked, 0)) * 0.2 +
             (pk.valid_pks * 100.0 / NULLIF(pk.total_tables_checked, 0)) * 0.2),
            1
        ) >= 90 THEN '🟡 B - GOOD'
        WHEN ROUND(
            (c.completeness_score * 0.3 + 
             r.reconciliation_score * 0.3 + 
             ((n.total_columns_checked - n.critical_null_issues - n.high_null_issues) * 100.0 / 
              NULLIF(n.total_columns_checked, 0)) * 0.2 +
             (pk.valid_pks * 100.0 / NULLIF(pk.total_tables_checked, 0)) * 0.2),
            1
        ) >= 80 THEN '🟠 C - ACCEPTABLE'
        ELSE '🔴 D - NEEDS IMPROVEMENT'
    END AS overall_grade,
    
    CURRENT_TIMESTAMP() AS snapshot_time
    
FROM completeness c
CROSS JOIN reconciliation r
CROSS JOIN null_analysis n
CROSS JOIN pk_validation pk;
```

### **TILE 19: Data Integrity Issues**

**Purpose:** Track PK/FK violations, duplicates, null rates  
**Type:** Table with severity  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Data Integrity Issues - All Integrity Checks
-- ═══════════════════════════════════════════════════════════════════════════════
WITH pk_issues AS (
    SELECT 
        'PRIMARY_KEY' AS issue_type,
        table_name AS affected_object,
        pk_column AS affected_column,
        duplicate_count AS issue_count,
        'Duplicate primary keys found' AS issue_description,
        CASE 
            WHEN duplicate_count > 100 THEN 'CRITICAL'
            WHEN duplicate_count > 10 THEN 'HIGH'
            ELSE 'MEDIUM'
        END AS severity,
        validated_at AS detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION
    WHERE pk_status != '✅ VALID'
),
fk_issues AS (
    SELECT 
        'FOREIGN_KEY' AS issue_type,
        relationship AS affected_object,
        fk_column AS affected_column,
        orphan_fk_count AS issue_count,
        'Orphaned foreign keys found' AS issue_description,
        CASE 
            WHEN orphan_fk_count > 100 THEN 'CRITICAL'
            WHEN orphan_fk_count > 10 THEN 'HIGH'
            ELSE 'MEDIUM'
        END AS severity,
        validated_at AS detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_FK_VALIDATION
    WHERE fk_status != '✅ VALID'
),
duplicate_issues AS (
    SELECT 
        'DUPLICATE' AS issue_type,
        table_name AS affected_object,
        business_key AS affected_column,
        total_duplicate_rows AS issue_count,
        'Business key duplicates found' AS issue_description,
        CASE 
            WHEN total_duplicate_rows > 100 THEN 'CRITICAL'
            WHEN total_duplicate_rows > 10 THEN 'HIGH'
            ELSE 'MEDIUM'
        END AS severity,
        checked_at AS detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DUPLICATE_DETECTION
    WHERE duplicate_status != '✅ NO DUPLICATES'
),
null_issues AS (
    SELECT 
        'NULL_RATE' AS issue_type,
        table_name AS affected_object,
        column_name AS affected_column,
        null_count AS issue_count,
        'High null rate: ' || null_rate_pct || '%' AS issue_description,
        CASE 
            WHEN quality_status LIKE '%CRITICAL%' THEN 'CRITICAL'
            WHEN quality_status LIKE '%HIGH%' THEN 'HIGH'
            ELSE 'MEDIUM'
        END AS severity,
        analyzed_at AS detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_NULL_TREND_ANALYSIS
    WHERE quality_status NOT LIKE '%GOOD%'
)
SELECT 
    issue_type,
    affected_object,
    affected_column,
    issue_count,
    issue_description,
    severity,
    detected_at,
    DATEDIFF('hour', detected_at, CURRENT_TIMESTAMP()) AS hours_since_detected,
    
    -- Priority
    CASE 
        WHEN severity = 'CRITICAL' AND issue_count > 100 THEN '🔴 P0 - URGENT'
        WHEN severity = 'CRITICAL' THEN '🔴 P1 - HIGH'
        WHEN severity = 'HIGH' THEN '🟠 P2 - MEDIUM'
        ELSE '🟡 P3 - LOW'
    END AS priority
    
FROM (
    SELECT * FROM pk_issues
    UNION ALL
    SELECT * FROM fk_issues
    UNION ALL
    SELECT * FROM duplicate_issues
    UNION ALL
    SELECT * FROM null_issues
) all_issues
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    issue_count DESC,
    detected_at DESC
LIMIT 100;
```

---

## 💰 Cost & Resource Optimization

### **TILE 20: Cost Dashboard**

**Purpose:** Complete cost tracking and optimization opportunities  
**Type:** Multi-metric with trends  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Cost Dashboard - Complete Cost Analysis
-- ═══════════════════════════════════════════════════════════════════════════════
WITH daily_cost AS (
    SELECT 
        SUM(estimated_cost_usd) AS today_cost,
        AVG(credits_7day_avg * 3.0) AS avg_daily_cost_7d
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY
    WHERE usage_date = CURRENT_DATE() - 1
),
monthly_cost AS (
    SELECT 
        total_cost_usd AS mtd_cost,
        prev_month_cost_usd,
        mom_cost_change_pct,
        budget_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY
    WHERE month = DATE_TRUNC('month', CURRENT_DATE())
),
top_models AS (
    SELECT 
        SUM(estimated_cost_usd) AS top_10_models_cost
    FROM (
        SELECT estimated_cost_usd
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
        ORDER BY estimated_cost_usd DESC
        LIMIT 10
    )
),
anomalies AS (
    SELECT 
        COUNT(*) AS cost_anomaly_count
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST
    WHERE usage_date >= DATEADD('day', -7, CURRENT_DATE())
    AND severity IN ('CRITICAL', 'HIGH')
)
SELECT 
    -- Daily
    dc.today_cost,
    dc.avg_daily_cost_7d,
    ROUND((dc.today_cost - dc.avg_daily_cost_7d) * 100.0 / NULLIF(dc.avg_daily_cost_7d, 0), 1) AS daily_variance_pct,
    
    -- Monthly
    mc.mtd_cost,
    mc.prev_month_cost_usd,
    mc.mom_cost_change_pct,
    mc.budget_status,
    
    -- Optimization
    tm.top_10_models_cost,
    ROUND(tm.top_10_models_cost * 100.0 / NULLIF(mc.mtd_cost, 0), 1) AS top_10_pct_of_total,
    
    -- Anomalies
    a.cost_anomaly_count,
    
    -- Projected
    ROUND(mc.mtd_cost / DAY(CURRENT_DATE()) * DAY(LAST_DAY(CURRENT_DATE())), 2) AS projected_month_cost,
    
    -- Health
    CASE 
        WHEN a.cost_anomaly_count > 0 THEN '🔴 ANOMALIES DETECTED'
        WHEN mc.mom_cost_change_pct > 20 THEN '🟠 HIGH GROWTH (>20%)'
        WHEN mc.mom_cost_change_pct > 10 THEN '🟡 MODERATE GROWTH (>10%)'
        ELSE '🟢 STABLE'
    END AS cost_health
    
FROM daily_cost dc
CROSS JOIN monthly_cost mc
CROSS JOIN top_models tm
CROSS JOIN anomalies a;
```

### **TILE 21: Top Cost Models**

**Purpose:** Identify expensive models for optimization  
**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Top Cost Models - Optimization Targets
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    cost_rank,
    model_name,
    schema_name,
    
    -- Cost Metrics
    estimated_cost_usd,
    cost_per_execution,
    cost_tier,
    
    -- Execution Metrics
    executions,
    avg_seconds,
    total_seconds,
    
    -- Optimization Potential
    ROUND(estimated_cost_usd * 0.3, 2) AS potential_savings_30pct,
    
    -- Recommendations
    CASE 
        WHEN avg_seconds > 300 AND executions > 10 THEN '⚡ Optimize query performance'
        WHEN cost_per_execution > 1.0 THEN '💡 Review incremental strategy'
        WHEN executions > 50 THEN '📅 Consider scheduling optimization'
        ELSE '✅ Already optimized'
    END AS optimization_recommendation
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
ORDER BY estimated_cost_usd DESC
LIMIT 20;
```

### **TILE 22: Warehouse Resource Utilization**

**Purpose:** Track warehouse usage and efficiency  
**Type:** Table with utilization metrics  
**Refresh:** Every 30 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Warehouse Resource Utilization
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    warehouse_name,
    warehouse_size,
    
    -- Daily Metrics (yesterday)
    (SELECT estimated_cost_usd 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_CREDITS
     WHERE warehouse_name = wc.warehouse_name 
       AND usage_date = CURRENT_DATE() - 1
     LIMIT 1) AS yesterday_cost,
    
    -- Utilization
    query_count,
    ROUND(compute_hours, 2) AS compute_hours,
    utilization_pct,
    utilization_status,
    
    -- Recommendations
    sizing_recommendation,
    
    -- Efficiency Score
    CASE 
        WHEN utilization_pct >= 70 AND utilization_pct <= 90 THEN '🟢 OPTIMAL (70-90%)'
        WHEN utilization_pct >= 50 AND utilization_pct < 70 THEN '🟡 UNDERUTILIZED (<70%)'
        WHEN utilization_pct > 90 THEN '🟠 OVERUTILIZED (>90%)'
        ELSE '🔴 VERY LOW (<50%)'
    END AS efficiency_status
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_UTILIZATION wc
WHERE hour_bucket >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 
    warehouse_name,
    warehouse_size,
    query_count,
    compute_hours,
    utilization_pct,
    utilization_status,
    sizing_recommendation
ORDER BY compute_hours DESC;
```

---

## 🏗️ Infrastructure Health

### **TILE 23: Infrastructure Health Summary**

**Purpose:** Overall infrastructure status  
**Type:** Scorecard  
**Refresh:** Every 15 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Infrastructure Health Summary
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    -- Warehouse Health
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS warehouse_issues,
    
    -- Security Health
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS security_issues,
    
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_FAILED_LOGINS
     WHERE event_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS failed_logins_24h,
    
    -- Storage Health
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STORAGE 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS storage_issues,
    
    (SELECT ROUND(SUM(size_gb), 2) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_USAGE) AS total_storage_gb,
    
    -- Task Health
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_TASKS 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS task_issues,
    
    (SELECT ROUND(AVG(success_rate_pct), 1) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TASK_PERFORMANCE) AS avg_task_success_rate,
    
    -- Concurrency Health
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_CONTENTION 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS contention_issues,
    
    -- Overall Status
    CASE 
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY WHERE severity = 'CRITICAL') > 0 
        THEN '🔴 CRITICAL - Security Issue'
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE WHERE severity = 'CRITICAL') > 0 
        THEN '🔴 CRITICAL - Warehouse Issue'
        WHEN ((SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE WHERE severity = 'HIGH') +
              (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY WHERE severity = 'HIGH') +
              (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STORAGE WHERE severity = 'HIGH')) > 0 
        THEN '🟠 WARNING - Multiple High Issues'
        ELSE '🟢 HEALTHY'
    END AS infrastructure_status,
    
    CURRENT_TIMESTAMP() AS snapshot_time;
```

### **TILE 24: Storage Growth Forecast**

**Purpose:** Predict future storage needs  
**Type:** Line chart with projection  
**Refresh:** Daily

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Storage Growth Forecast
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    database_name,
    
    -- Current State
    current_gb,
    
    -- Growth Metrics
    avg_daily_growth_gb,
    
    -- Forecasts
    forecast_30d_gb,
    forecast_30d_cost_usd,
    forecast_90d_gb,
    forecast_90d_cost_usd,
    forecast_1yr_gb,
    forecast_1yr_monthly_cost_usd,
    
    -- Growth Rate
    ROUND(avg_daily_growth_gb * 100.0 / NULLIF(current_gb, 0), 2) AS daily_growth_rate_pct,
    
    -- Status
    growth_status,
    
    -- Action Required
    CASE 
        WHEN forecast_1yr_gb > current_gb * 3 THEN '⚠️ High growth - review retention'
        WHEN forecast_1yr_gb > current_gb * 2 THEN '📊 Moderate growth - monitor'
        ELSE '✅ Stable growth'
    END AS action_recommendation
    
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_FORECAST;
```

---

## 🚨 Alert Management

### **TILE 25: Active Alerts - All Categories**

**Purpose:** Unified view of all active alerts  
**Type:** Table with priority  
**Refresh:** Every 5 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- Active Alerts - All Categories Unified
-- ═══════════════════════════════════════════════════════════════════════════════
WITH all_alerts AS (
    -- Performance Alerts
    SELECT 
        'PERFORMANCE' AS category,
        severity,
        model_name AS affected_object,
        'Model running ' || percent_slower || '% slower than baseline' AS alert_message,
        alert_time AS triggered_at
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
    
    UNION ALL
    
    -- Data Integrity Alerts
    SELECT 
        'DATA_INTEGRITY',
        severity,
        table_name,
        alert_description,
        detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
    WHERE severity IN ('CRITICAL', 'HIGH')
    
    UNION ALL
    
    -- Cost Alerts
    SELECT 
        'COST_ANOMALY',
        severity,
        warehouse_name,
        alert_description,
        detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST
    WHERE severity IN ('CRITICAL', 'HIGH')
    
    UNION ALL
    
    -- Infrastructure Alerts
    SELECT 
        'WAREHOUSE',
        severity,
        warehouse_name,
        alert_description,
        detected_at
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE
    WHERE severity IN ('CRITICAL', 'HIGH')
)
SELECT 
    category,
    severity,
    affected_object,
    alert_message,
    triggered_at,
    
    -- Time Open
    DATEDIFF('minute', triggered_at, CURRENT_TIMESTAMP()) AS minutes_open,
    CASE 
        WHEN DATEDIFF('minute', triggered_at, CURRENT_TIMESTAMP()) > 1440 THEN '🔴 >24h'
        WHEN DATEDIFF('minute', triggered_at, CURRENT_TIMESTAMP()) > 240 THEN '🟠 >4h'
        WHEN DATEDIFF('minute', triggered_at, CURRENT_TIMESTAMP()) > 60 THEN '🟡 >1h'
        ELSE '🟢 Recent'
    END AS age_status,
    
    -- Priority
    CASE 
        WHEN severity = 'CRITICAL' AND category IN ('MODEL_FAILURE', 'DATA_INTEGRITY') THEN '🔴 P0 - URGENT'
        WHEN severity = 'CRITICAL' THEN '🔴 P1 - HIGH'
        WHEN severity = 'HIGH' AND category IN ('MODEL_FAILURE', 'PERFORMANCE') THEN '🟠 P1 - HIGH'
        WHEN severity = 'HIGH' THEN '🟠 P2 - MEDIUM'
        ELSE '🟡 P3 - LOW'
    END AS priority
    
FROM all_alerts
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    triggered_at DESC
LIMIT 100;
```

---

## 🎨 Dashboard Setup Guide

### **Step 1: Prerequisites**

Ensure all monitoring setup scripts have been executed:

```bash
✅ O2C_ENHANCED_AUDIT_SETUP.sql
✅ O2C_ENHANCED_TELEMETRY_SETUP.sql  
✅ O2C_ENHANCED_MONITORING_SETUP.sql
✅ O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql
✅ O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql
✅ O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql
```

### **Step 2: Create Dashboard in Snowsight**

1. Log into Snowsight
2. Navigate to **Dashboards** → **+ Dashboard**
3. Name: "O2C Enhanced - Unified Monitoring"
4. Description: "Complete observability and monitoring dashboard"

### **Step 3: Add Tiles in Order**

For each tile (1-25):
1. Click **+ Add Tile** → **From SQL Query**
2. Copy-paste the SQL query
3. Click **Run**
4. Configure visualization type (Scorecard, Table, Line Chart, etc.)
5. Set tile title
6. Set refresh schedule (as noted in each tile)
7. Click **Add to Dashboard**

### **Step 4: Recommended Layout**

```
┌──────────────────────────────────────────────────────────────────────────┐
│  TILE 1: Platform Health Overview (Full Width Scorecard)                 │
├────────────────────────────────┬─────────────────────────────────────────┤
│ TILE 2: Daily Run Summary      │ TILE 5: Model Performance Dashboard     │
├────────────────────────────────┼─────────────────────────────────────────┤
│ TILE 8: Error Dashboard        │ TILE 12: Test Execution Dashboard       │
├────────────────────────────────┼─────────────────────────────────────────┤
│ TILE 16: Data Freshness        │ TILE 18: Data Quality Scorecard         │
├────────────────────────────────┼─────────────────────────────────────────┤
│ TILE 20: Cost Dashboard        │ TILE 23: Infrastructure Health          │
├──────────────────────────────────────────────────────────────────────────┤
│  TILE 25: Active Alerts - All Categories (Full Width Table)             │
└──────────────────────────────────────────────────────────────────────────┘
```

### **Step 5: Set Up Alerts (Optional)**

Configure email notifications for critical tiles:
- TILE 1: Platform Health < 70
- TILE 25: Any P0 alerts appear

---

## ✅ Summary

### **Total Tiles: 25**

| Category | Tiles | Coverage |
|----------|-------|----------|
| **Executive Health** | 1 | Overall platform status |
| **Run Metrics** | 3 | Daily runs, timeline, details |
| **Model Performance** | 3 | Performance, compilation, build |
| **Error Analysis** | 4 | Errors, trends, failures, root cause |
| **Test & DQ** | 4 | Test execution, coverage, recurring failures |
| **Data Observability** | 4 | Freshness, reconciliation, quality, integrity |
| **Cost & Resources** | 3 | Cost tracking, optimization, warehouse |
| **Infrastructure** | 2 | Health summary, storage forecast |
| **Alert Management** | 1 | Unified active alerts |

### **Coverage Checklist**

✅ **Run Metrics:** Daily summary, run-level details, execution timeline  
✅ **Model Metrics:** Performance dashboard, compilation analysis, build metrics  
✅ **Error Analysis:** Error dashboard, trend analysis, model failures, root cause  
✅ **DQ Test Cases:** Test execution, pass rates, coverage, recurring failures  
✅ **Data Observability:** Freshness, reconciliation, quality scorecard, integrity  
✅ **Cost Optimization:** Complete cost tracking, top models, warehouse utilization  
✅ **Infrastructure:** Health summary, storage forecast  
✅ **Alert Management:** Unified active alerts across all categories  

### **Key Features**

- ✅ **Zero Duplication:** Single source of truth for each metric
- ✅ **Complete Coverage:** All observability dimensions included
- ✅ **Actionable Insights:** Every tile includes status indicators and recommendations
- ✅ **Prioritization:** Severity levels and priority flags on all alerts
- ✅ **Trend Analysis:** Moving averages and anomaly detection built-in
- ✅ **Cost Awareness:** Resource optimization guidance throughout

---

**Ready for Production!** 🚀

**Last Updated:** January 2025  
**Version:** 1.0.0  
**Status:** ✅ Complete and Production Ready

