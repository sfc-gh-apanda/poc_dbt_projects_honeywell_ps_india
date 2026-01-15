# O2C Enhanced - Complete Monitoring & Observability Dashboard

**Purpose:** Consolidated dashboard queries for comprehensive O2C Enhanced monitoring  
**Coverage:** All metrics for Project Deployment, Testing, Data Quality, Performance, Cost, Telemetry  
**Platform:** Snowsight / Tableau / Power BI / Monte Carlo  
**Updated:** January 2026  

---

## 📋 What You Need - Quick Reference

### Your Requirements:
1. ✅ **Project Deployment / Compile / Run Metrics**
2. ✅ **Test Validation Metrics**  
3. ✅ **Error / Log Metrics and Analysis**
4. ✅ **Data Quality and Observability Metrics**
5. ✅ **Model Metrics (performance, cost, etc.)**
6. ✅ **Telemetry**

### One File to Rule Them All:
This file contains **33 dashboard tiles** organized into **8 categories** covering all your observability needs.

---

## 📊 Dashboard Categories & Coverage

| Category | Tiles | What It Covers |
|----------|-------|----------------|
| **1. Executive Summary** | 2 tiles | Platform health score, observability KPIs |
| **2. Cost Monitoring** | 4 tiles | Daily costs, cost by model, monthly trends, anomalies |
| **3. Query Performance** | 4 tiles | Queue times, long queries, compilation analysis |
| **4. Model Performance** | 4 tiles | Model trends, efficiency, degradation alerts, slowest models |
| **5. Schema Drift** | 4 tiles | Current state, DDL changes, table modifications |
| **6. dbt Observability** | 5 tiles | Test coverage, dependencies, run history, orphan models |
| **7. Data Integrity** | 6 tiles | PK/FK validation, duplicates, null rates, consistency |
| **8. Alerts** | 4 tiles | Active alerts, history, trends, summaries |

**Total: 33 Dashboard Tiles**

---

## 🎯 Category 1: Executive Summary & Platform Health

### TILE 1: Platform Health Summary Scorecard

**Metrics:** Health score, critical alerts, cost, integrity issues  
**Refresh:** Every 5 minutes

```sql
-- Platform Health Summary Scorecard
SELECT 
    h.health_score,
    h.health_status,
    h.total_critical_alerts,
    h.critical_performance_issues,
    h.critical_model_failures,
    h.critical_stale_sources,
    
    -- Cost metrics
    (SELECT ROUND(SUM(estimated_cost_usd), 2) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY 
     WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE())) AS mtd_cost_usd,
    
    -- Data integrity
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION 
     WHERE pk_status != '✅ VALID') AS integrity_issues,
    
    -- Active alerts
    (SELECT COUNT(*) FROM EDW.O2C_AUDIT.V_ACTIVE_ALERTS) AS active_alerts,
    
    h.snapshot_time
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY h;
```

---

### TILE 2: Observability KPI Grid

**Metrics:** All key metrics across cost, performance, schema, dbt, integrity  
**Refresh:** Every 15 minutes

```sql
-- Observability KPIs
SELECT
    -- Cost KPIs
    (SELECT ROUND(SUM(estimated_cost_usd), 2) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY 
     WHERE usage_date = CURRENT_DATE() - 1) AS yesterday_cost_usd,
    
    (SELECT ROUND(total_cost_usd, 2) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY 
     WHERE month = DATE_TRUNC('month', CURRENT_DATE())) AS mtd_cost_usd,
    
    -- Performance KPIs
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS perf_degradation_count,
    
    (SELECT ROUND(AVG(avg_queue_seconds), 1) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_QUEUE_TIME_ANALYSIS 
     WHERE hour_bucket >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS avg_queue_seconds_24h,
    
    -- Schema KPIs
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DDL_CHANGES 
     WHERE change_time >= DATEADD('day', -7, CURRENT_DATE())) AS schema_changes_7d,
    
    -- dbt KPIs
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE 
     WHERE coverage_status = '❌ NO TESTS') AS models_without_tests,
    
    -- Integrity KPIs
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS integrity_issues,
    
    CURRENT_TIMESTAMP() AS snapshot_time;
```

---

## 💰 Category 2: Cost Monitoring

### TILE 3: Daily Cost Trend (Last 30 Days)

**Type:** Line chart with 7-day moving average  
**Refresh:** Daily

```sql
-- Daily Cost Trend with 7-Day Moving Average
SELECT 
    usage_date,
    warehouse_name,
    estimated_credits,
    estimated_cost_usd,
    credits_7day_avg,
    ROUND(credits_7day_avg * 3.0, 2) AS cost_7day_avg_usd,
    variance_from_avg_pct,
    CASE 
        WHEN variance_from_avg_pct > 50 THEN 'SPIKE'
        WHEN variance_from_avg_pct < -50 THEN 'DIP'
        ELSE 'NORMAL'
    END AS cost_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY
WHERE usage_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY usage_date DESC, warehouse_name;
```

---

### TILE 4: Cost by Model (Top 15)

**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- Top 15 Most Expensive Models (Last 7 Days)
SELECT 
    model_name,
    schema_name,
    executions,
    avg_seconds,
    total_seconds,
    estimated_cost_usd,
    cost_per_execution,
    cost_tier,
    cost_rank
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
ORDER BY estimated_cost_usd DESC
LIMIT 15;
```

---

### TILE 5: Monthly Cost Summary

**Type:** Table with MoM comparison  
**Refresh:** Daily

```sql
-- Monthly Cost Summary with MoM Comparison
SELECT 
    month,
    total_credits,
    total_cost_usd,
    total_queries,
    total_gb_scanned,
    active_days,
    avg_daily_cost_usd,
    prev_month_cost_usd,
    mom_cost_change_pct,
    budget_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY
ORDER BY month DESC
LIMIT 12;
```

---

### TILE 6: Cost Anomaly Alerts

**Type:** Table with severity  
**Refresh:** Every hour

```sql
-- Cost Anomaly Alerts (Last 7 Days)
SELECT 
    usage_date,
    warehouse_name,
    estimated_cost_usd,
    credits_7day_avg,
    variance_from_avg_pct,
    severity,
    alert_description,
    detected_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST
WHERE usage_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    usage_date DESC;
```

---

## ⚡ Category 3: Query Performance

### TILE 7: Queue Time Heatmap

**Type:** Heatmap or table  
**Refresh:** Every 30 minutes

```sql
-- Queue Time Analysis (Last 7 Days)
SELECT 
    DATE(hour_bucket) AS date,
    HOUR(hour_bucket) AS hour,
    warehouse_name,
    query_count,
    avg_queue_seconds,
    max_queue_seconds,
    p95_queue_seconds,
    queue_rate_pct,
    queue_status,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_QUEUE_TIME_ANALYSIS
WHERE hour_bucket >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY hour_bucket DESC;
```

---

### TILE 8: Long Running Queries

**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- Long Running Queries (Last 24 Hours)
SELECT 
    query_id,
    start_time,
    ROUND(elapsed_seconds / 60, 1) AS duration_minutes,
    execution_seconds,
    compilation_seconds,
    queue_seconds,
    warehouse_name,
    warehouse_size,
    user_name,
    mb_scanned,
    rows_produced,
    severity,
    bottleneck_analysis,
    LEFT(query_preview, 200) AS query_preview
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_LONG_RUNNING_QUERIES
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY elapsed_seconds DESC
LIMIT 25;
```

---

### TILE 9: Compilation Time Trend

**Type:** Line chart  
**Refresh:** Daily

```sql
-- Compilation Time Trend (Last 14 Days)
SELECT 
    query_date,
    query_count,
    avg_compilation_seconds,
    median_compilation_seconds,
    p95_compilation_seconds,
    p99_compilation_seconds,
    slow_compile_count,
    slow_compile_pct,
    total_compilation_minutes,
    compile_health
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COMPILATION_ANALYSIS
ORDER BY query_date DESC;
```

---

### TILE 10: Queue Time Alerts

**Type:** Table with severity  
**Refresh:** Every 30 minutes

```sql
-- Queue Time Alerts (Last 6 Hours)
SELECT 
    hour_bucket,
    warehouse_name,
    warehouse_size,
    avg_queue_seconds,
    max_queue_seconds,
    query_count,
    queue_rate_pct,
    severity,
    alert_description,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_QUEUE
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    avg_queue_seconds DESC;
```

---

## 📈 Category 4: Model Performance

### TILE 11: Model Performance Trend

**Type:** Table with trend indicators  
**Refresh:** Every 4 hours

```sql
-- Model Performance Trend (Last 14 Days) - Latest per Model
WITH latest_per_model AS (
    SELECT 
        model_name,
        schema_name,
        run_date,
        avg_seconds,
        max_seconds,
        avg_7day_ma,
        baseline_avg,
        variance_from_baseline_pct,
        performance_trend,
        ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY run_date DESC) AS rn
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_PERFORMANCE_TREND
)
SELECT 
    model_name,
    schema_name,
    run_date AS latest_run,
    avg_seconds AS latest_avg_seconds,
    avg_7day_ma,
    baseline_avg,
    variance_from_baseline_pct,
    performance_trend
FROM latest_per_model
WHERE rn = 1
ORDER BY 
    CASE performance_trend
        WHEN '🔴 DEGRADED (>50% slower)' THEN 1
        WHEN '🟠 SLOWING (>20% slower)' THEN 2
        WHEN '🟢 IMPROVED (>20% faster)' THEN 3
        ELSE 4
    END,
    avg_seconds DESC;
```

---

### TILE 12: Incremental Model Efficiency

**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- Incremental Model Efficiency Analysis
SELECT 
    model_name,
    schema_name,
    load_strategy,
    run_count,
    avg_seconds,
    avg_rows,
    rows_per_second,
    seconds_per_1k_rows,
    cost_per_1k_rows,
    efficiency_status,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
WHERE load_strategy NOT IN ('VIEW (No Load)', 'UNKNOWN')
ORDER BY 
    CASE efficiency_status
        WHEN '🔴 INEFFICIENT (<100/s) - Review' THEN 1
        WHEN '🟠 SLOW (100-1K/s)' THEN 2
        WHEN '🟡 MODERATE (1-10K/s)' THEN 3
        ELSE 4
    END,
    rows_per_second ASC NULLS LAST;
```

---

### TILE 13: Performance Degradation Alerts

**Type:** Table with severity  
**Refresh:** Every 4 hours

```sql
-- Model Performance Degradation Alerts
SELECT 
    model_name,
    baseline_seconds,
    recent_avg_seconds,
    recent_max_seconds,
    seconds_slower,
    percent_slower,
    severity,
    recent_run_count,
    alert_time
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    percent_slower DESC;
```

---

### TILE 14: Slowest Models

**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- Top 20 Slowest Models
SELECT 
    model_name,
    schema_name,
    run_count,
    avg_seconds,
    max_seconds,
    min_seconds,
    total_seconds,
    estimated_cost_usd,
    performance_tier
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
ORDER BY avg_seconds DESC
LIMIT 20;
```

---

## 🔄 Category 5: Schema Drift Detection

### TILE 15: Schema Current State

**Type:** Table  
**Refresh:** Hourly

```sql
-- Schema Current State (Summary by Table)
SELECT 
    schema_name,
    table_name,
    object_type,
    COUNT(*) AS column_count,
    MAX(table_row_count) AS row_count,
    MAX(table_size_mb) AS size_mb,
    MAX(table_last_altered) AS last_altered,
    DATEDIFF('day', MAX(table_last_altered), CURRENT_TIMESTAMP()) AS days_since_change
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SCHEMA_CURRENT_STATE
GROUP BY schema_name, table_name, object_type
ORDER BY schema_name, table_name;
```

---

### TILE 16: DDL Change History

**Type:** Table  
**Refresh:** Every 30 minutes

```sql
-- DDL Change History (Last 30 Days)
SELECT 
    change_time,
    user_name,
    ddl_operation,
    object_type,
    affected_object,
    execution_status,
    impact_level,
    severity,
    LEFT(query_text_preview, 200) AS query_preview
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DDL_CHANGES
ORDER BY change_time DESC
LIMIT 50;
```

---

### TILE 17: Recent Table Changes

**Type:** Table with status indicators  
**Refresh:** Hourly

```sql
-- Recent Table Changes (Last 30 Days)
SELECT 
    schema_name,
    table_name,
    last_schema_change,
    current_row_count,
    current_size_mb,
    column_count,
    change_status,
    days_since_change
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COLUMN_CHANGES
WHERE days_since_change <= 30
ORDER BY last_schema_change DESC;
```

---

### TILE 18: Schema Drift Alerts

**Type:** Table with severity  
**Refresh:** Every 30 minutes

```sql
-- Schema Drift Alerts (Last 7 Days)
SELECT 
    alert_type,
    detected_at,
    affected_object,
    change_description,
    severity,
    impact_level,
    changed_by,
    LEFT(details, 200) AS details
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SCHEMA_DRIFT
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    detected_at DESC
LIMIT 30;
```

---

## 🧪 Category 6: dbt Observability

### TILE 19: Test Coverage by Model

**Type:** Table with coverage indicators  
**Refresh:** Daily

```sql
-- dbt Test Coverage Analysis
SELECT 
    model_name,
    schema_name,
    last_model_run,
    test_count,
    passed_tests,
    failed_tests,
    pass_rate_pct,
    coverage_status,
    test_health
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE
ORDER BY 
    CASE coverage_status
        WHEN '❌ NO TESTS' THEN 1
        WHEN '🟡 LOW COVERAGE (1 test)' THEN 2
        WHEN '🟢 MODERATE COVERAGE (2-4 tests)' THEN 3
        ELSE 4
    END,
    CASE test_health
        WHEN '🔴 FAILING' THEN 1
        ELSE 2
    END,
    model_name;
```

---

### TILE 20: Model Dependencies

**Type:** Table  
**Refresh:** Daily

```sql
-- Model Dependencies and Complexity
SELECT 
    model_name,
    primary_sources,
    source_count,
    complexity_indicator
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_MODEL_DEPENDENCIES
ORDER BY source_count DESC;
```

---

### TILE 21: dbt Run History

**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- dbt Run History (Last 30 Days)
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
    success_rate_pct,
    duration_category,
    run_health,
    warehouse_name,
    user_name
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_RUN_HISTORY
ORDER BY run_started_at DESC
LIMIT 50;
```

---

### TILE 22: Orphan/Stale Models

**Type:** Table  
**Refresh:** Daily

```sql
-- Orphan and Stale Models
SELECT 
    schema_name,
    table_name,
    created_at,
    last_altered,
    last_activity,
    executions_30d,
    days_inactive,
    activity_status,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_ORPHAN_MODELS
WHERE activity_status NOT IN ('⚪ ACTIVE', '🟢 MODERATE (>7 days inactive)')
ORDER BY days_inactive DESC;
```

---

### TILE 23: dbt Coverage Alerts

**Type:** Table with severity  
**Refresh:** Daily

```sql
-- dbt Coverage and Health Alerts
SELECT 
    alert_type,
    model_name,
    schema_name,
    coverage_status,
    severity,
    alert_description,
    detected_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DBT_COVERAGE
ORDER BY 
    CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    alert_type,
    model_name;
```

---

## 🔒 Category 7: Data Integrity & Quality

### TILE 24: Primary Key Validation

**Type:** Table with status  
**Refresh:** Daily

```sql
-- Primary Key Validation Status
SELECT 
    table_name,
    pk_column,
    total_rows,
    unique_keys,
    duplicate_count,
    null_pk_count,
    uniqueness_pct,
    pk_status,
    validated_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION
ORDER BY 
    CASE pk_status WHEN '✅ VALID' THEN 2 ELSE 1 END,
    table_name;
```

---

### TILE 25: Foreign Key Validation

**Type:** Table with status  
**Refresh:** Daily

```sql
-- Foreign Key Referential Integrity
SELECT 
    relationship,
    fk_column,
    fk_distinct_values,
    pk_distinct_values,
    orphan_fk_count,
    fk_status,
    validated_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_FK_VALIDATION
ORDER BY 
    CASE fk_status WHEN '✅ VALID' THEN 2 ELSE 1 END;
```

---

### TILE 26: Duplicate Detection

**Type:** Table  
**Refresh:** Daily

```sql
-- Duplicate Detection Status
SELECT 
    table_name,
    business_key,
    duplicate_groups,
    total_duplicate_rows,
    max_duplicates_per_key,
    duplicate_status,
    checked_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DUPLICATE_DETECTION
ORDER BY 
    CASE duplicate_status WHEN '✅ NO DUPLICATES' THEN 3 ELSE 1 END,
    total_duplicate_rows DESC;
```

---

### TILE 27: Null Rate Analysis

**Type:** Table with quality indicators  
**Refresh:** Daily

```sql
-- Null Rate Analysis for Critical Columns
SELECT 
    table_name,
    column_name,
    column_importance,
    total_rows,
    null_count,
    null_rate_pct,
    quality_status,
    analyzed_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_NULL_TREND_ANALYSIS
ORDER BY 
    CASE quality_status
        WHEN '🔴 CRITICAL (>20%)' THEN 1
        WHEN '🔴 HIGH NULLS' THEN 2
        WHEN '🔴 NULLS IN DIMENSION' THEN 3
        WHEN '🟠 HIGH (5-20%)' THEN 4
        ELSE 5
    END,
    null_rate_pct DESC;
```

---

### TILE 28: Data Consistency Checks

**Type:** Table with status  
**Refresh:** Hourly

```sql
-- Data Consistency (Source to Target Validation)
SELECT 
    validation_layer,
    entity,
    source_count,
    target_count,
    variance,
    variance_pct,
    consistency_status,
    checked_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DATA_CONSISTENCY
ORDER BY 
    CASE consistency_status
        WHEN '✅ MATCHED' THEN 4
        WHEN '🟢 MINOR VARIANCE (<1%)' THEN 3
        WHEN '🟡 MODERATE VARIANCE (1-5%)' THEN 2
        ELSE 1
    END,
    ABS(variance_pct) DESC;
```

---

### TILE 29: Data Integrity Alerts

**Type:** Table with severity  
**Refresh:** Daily

```sql
-- Data Integrity Alerts (All Types)
SELECT 
    alert_type,
    table_name,
    affected_column,
    issue,
    severity,
    alert_description,
    detected_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    detected_at DESC;
```

---

## 🚨 Category 8: Alert Management

### TILE 30: Active Alerts Dashboard

**Type:** Table with severity  
**Refresh:** Every 5 minutes

```sql
-- Active Alerts (Unresolved)
SELECT 
    alert_name,
    alert_type,
    severity,
    alert_message,
    affected_objects,
    triggered_at,
    status,
    DATEDIFF('hour', triggered_at, CURRENT_TIMESTAMP()) AS hours_open
FROM EDW.O2C_AUDIT.V_ACTIVE_ALERTS
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    triggered_at DESC;
```

---

### TILE 31: Alert History (Last 7 Days)

**Type:** Table  
**Refresh:** Every hour

```sql
-- Alert History (Last 7 Days)
SELECT 
    alert_id,
    alert_name,
    alert_type,
    severity,
    alert_message,
    triggered_at,
    acknowledged_at,
    acknowledged_by,
    resolved_at,
    CASE 
        WHEN resolved_at IS NOT NULL THEN 'RESOLVED'
        WHEN acknowledged_at IS NOT NULL THEN 'ACKNOWLEDGED'
        ELSE 'OPEN'
    END AS status,
    CASE 
        WHEN resolved_at IS NOT NULL 
        THEN DATEDIFF('minute', triggered_at, resolved_at)
        ELSE NULL
    END AS resolution_time_minutes
FROM EDW.O2C_AUDIT.O2C_ALERT_HISTORY
WHERE triggered_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY triggered_at DESC;
```

---

### TILE 32: Alert Trend by Type

**Type:** Stacked bar chart  
**Refresh:** Daily

```sql
-- Alert Trend by Type (Last 30 Days)
SELECT 
    DATE(triggered_at) AS alert_date,
    alert_type,
    severity,
    COUNT(*) AS alert_count
FROM EDW.O2C_AUDIT.O2C_ALERT_HISTORY
WHERE triggered_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY alert_date, alert_type, severity
ORDER BY alert_date DESC, alert_count DESC;
```

---

### TILE 33: Alert Summary by Category

**Type:** Pie chart or table  
**Refresh:** Daily

```sql
-- Alert Summary by Category (Last 30 Days)
SELECT 
    alert_type AS category,
    COUNT(*) AS total_alerts,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical,
    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high,
    SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium,
    SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) AS low,
    SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) AS resolved,
    ROUND(SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS resolution_rate_pct
FROM EDW.O2C_AUDIT.O2C_ALERT_HISTORY
WHERE triggered_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY alert_type
ORDER BY total_alerts DESC;
```

---

## 🎨 Dashboard Setup in Snowsight

### Step 1: Prerequisites
```sql
-- Verify all monitoring views exist
SELECT TABLE_SCHEMA, TABLE_NAME, COMMENT
FROM EDW.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA IN ('O2C_ENHANCED_MONITORING', 'O2C_AUDIT')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Should return 81+ views
```

### Step 2: Create Dashboard
1. Log into Snowsight
2. Click **Dashboards** in left navigation
3. Click **+ Dashboard**
4. Name: "O2C Enhanced Complete Monitoring"
5. Description: "Comprehensive observability dashboard for O2C Enhanced"

### Step 3: Add Tiles
For each query above:
1. Click **+ Add Tile**
2. Select **From SQL Query**
3. Paste the SQL query
4. Click **Run**
5. Configure visualization (chart type, axes, colors)
6. Set refresh schedule
7. Save tile

### Step 4: Recommended Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  O2C ENHANCED - COMPLETE MONITORING DASHBOARD                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────┐  ┌────────────────────────────────────────────────┐ │
│  │ TILE 1: Health     │  │ TILE 2: Observability KPIs                     │ │
│  │ Score (Scorecard)  │  │ (Multi-metric)                                 │ │
│  └────────────────────┘  └────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐ │
│  │ TILE 3: Daily Cost Trend           │  │ TILE 4: Cost by Model          │ │
│  │ (Line Chart)                       │  │ (Bar Chart)                    │ │
│  └────────────────────────────────────┘  └────────────────────────────────┘ │
│                                                                              │
│  [Continue with remaining tiles in similar grid layout]                     │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ TILE 30: Active Alerts (Full Width Table)                            │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Summary & Quick Reference

### What This Dashboard Covers

| Your Requirement | Dashboard Tiles | Views Used |
|------------------|-----------------|------------|
| **Project Deployment / Compile / Run Metrics** | Tiles 19-23 | `O2C_ENH_DBT_*`, `V_DAILY_RUN_SUMMARY` |
| **Test Validation Metrics** | Tiles 19, 22, 23 | `O2C_ENH_DBT_TEST_COVERAGE`, `O2C_ENH_TEST_*` |
| **Error / Log Metrics** | Tiles 12, 13, 30-33 | `O2C_ENH_ERROR_*`, `O2C_ENH_BUILD_FAILURE_*` |
| **Data Quality & Observability** | Tiles 24-29 | `O2C_ENH_PK_VALIDATION`, `O2C_ENH_NULL_*`, etc. |
| **Model Metrics (performance, cost)** | Tiles 3-6, 11-14 | `O2C_ENH_COST_*`, `O2C_ENH_MODEL_PERFORMANCE_*` |
| **Telemetry** | Implicit in all tiles | `V_ROW_COUNT_TRACKING`, `V_BATCH_TRACKING` |

### Refresh Schedule Summary

| Category | Recommended Refresh |
|----------|---------------------|
| Health/Alerts | Every 5 minutes |
| Cost Daily | Daily |
| Cost Anomaly | Hourly |
| Queue Time | Every 30 minutes |
| Long Queries | Every 15 minutes |
| Model Performance | Every 4 hours |
| Schema Drift | Every 30 minutes |
| dbt Coverage | Daily |
| Data Integrity | Daily |
| Alert History | Hourly |

---

## 🔴 SECTION 9: ERROR ANALYSIS & DIAGNOSTICS

**Purpose:** Comprehensive error monitoring, root cause analysis, and failure tracking  
**Coverage:** Error logs, trends, build failures, recurring patterns, impact analysis  
**Views Used:** `O2C_ENH_ERROR_LOG`, `O2C_ENH_ERROR_TREND`, `O2C_ENH_BUILD_FAILURE_DETAILS`

---

### TILE 34: Error Log - Recent Failures

**Type:** Table with error categorization  
**Refresh:** Every 5 minutes

```sql
-- Detailed Error Log (Last 7 Days)
SELECT 
    query_id,
    error_time,
    user_name,
    warehouse_name,
    schema_name,
    query_type,
    error_code,
    error_message,
    error_category,
    LEFT(query_text_preview, 200) AS query_preview,
    DATEDIFF('hour', error_time, CURRENT_TIMESTAMP()) AS hours_ago,
    -- Visual indicator
    CASE error_category
        WHEN 'SYNTAX_ERROR' THEN '🔴 SYNTAX'
        WHEN 'OBJECT_NOT_FOUND' THEN '🟠 OBJECT MISSING'
        WHEN 'ACCESS_DENIED' THEN '🟡 ACCESS'
        WHEN 'INVALID_IDENTIFIER' THEN '🟣 IDENTIFIER'
        WHEN 'TIMEOUT' THEN '⚫ TIMEOUT'
        WHEN 'RESOURCE_LIMIT' THEN '🔵 RESOURCE'
        ELSE '⚪ OTHER'
    END AS category_display
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
ORDER BY error_time DESC
LIMIT 100;
```

**Error Categories:**
- `SYNTAX_ERROR` - SQL syntax issues
- `OBJECT_NOT_FOUND` - Missing tables/views/columns
- `ACCESS_DENIED` - Permission/privilege issues
- `INVALID_IDENTIFIER` - Column/object name errors
- `TIMEOUT` - Query execution timeouts
- `RESOURCE_LIMIT` - Memory/compute resource limits
- `OTHER` - Uncategorized errors

---

### TILE 35: Error Trend & Success Rate

**Type:** Dual-axis line chart  
**Refresh:** Every 15 minutes

```sql
-- Error Trend Analysis (Last 30 Days)
SELECT 
    date,
    total_queries,
    error_count,
    success_count,
    error_rate_pct,
    success_rate_pct,
    ROUND(error_rate_7day_avg, 2) AS error_rate_7day_avg,
    -- Health indicator
    CASE 
        WHEN error_rate_pct > 10 THEN '🔴 HIGH ERROR RATE (>10%)'
        WHEN error_rate_pct > 5 THEN '🟠 ELEVATED (5-10%)'
        WHEN error_rate_pct > 1 THEN '🟡 NORMAL (1-5%)'
        WHEN error_rate_pct > 0 THEN '🟢 GOOD (<1%)'
        ELSE '✅ EXCELLENT (0%)'
    END AS health_status,
    -- Trend indicator
    CASE 
        WHEN error_rate_pct > LAG(error_rate_pct) OVER (ORDER BY date) THEN '📈 INCREASING'
        WHEN error_rate_pct < LAG(error_rate_pct) OVER (ORDER BY date) THEN '📉 DECREASING'
        ELSE '➡️ STABLE'
    END AS trend
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_TREND
ORDER BY date DESC;
```

**Visualization:** 
- Line 1 (left axis): Error count
- Line 2 (right axis): Error rate %
- Line 3 (overlay): 7-day moving average

---

### TILE 36: Error Breakdown by Category

**Type:** Pie chart / Horizontal bar chart  
**Refresh:** Every hour

```sql
-- Error Distribution by Category
SELECT 
    error_category,
    COUNT(*) AS error_count,
    COUNT(DISTINCT user_name) AS affected_users,
    COUNT(DISTINCT schema_name) AS affected_schemas,
    COUNT(DISTINCT DATE(error_time)) AS days_with_errors,
    MIN(error_time) AS first_occurrence,
    MAX(error_time) AS last_occurrence,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total,
    -- Priority assessment
    CASE 
        WHEN COUNT(*) > 100 THEN '🔴 CRITICAL - Immediate action required'
        WHEN COUNT(*) > 50 THEN '🟠 HIGH - Review soon'
        WHEN COUNT(*) > 10 THEN '🟡 MEDIUM - Monitor'
        ELSE '🟢 LOW - Normal'
    END AS priority
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY error_category
ORDER BY error_count DESC;
```

---

### TILE 37: Build Failure Details (Enhanced)

**Type:** Table with severity and affected objects  
**Refresh:** Every 5 minutes

```sql
-- Comprehensive Build Failure Analysis
SELECT 
    failure_time,
    user_name,
    warehouse_name,
    schema_name,
    error_category,
    affected_object,
    error_code,
    LEFT(error_message, 150) AS error_summary,
    execution_seconds,
    minutes_ago,
    recency_severity,
    -- Actionable recommendation
    CASE error_category
        WHEN 'SYNTAX_ERROR' THEN '💡 Review SQL syntax'
        WHEN 'OBJECT_NOT_FOUND' THEN '💡 Check object existence and spelling'
        WHEN 'ACCESS_DENIED' THEN '💡 Verify role permissions'
        WHEN 'INVALID_IDENTIFIER' THEN '💡 Validate column/table names'
        WHEN 'TIMEOUT' THEN '💡 Optimize query or increase timeout'
        WHEN 'RESOURCE_LIMIT' THEN '💡 Scale up warehouse or optimize query'
        WHEN 'CONSTRAINT_VIOLATION' THEN '💡 Check data for duplicates/conflicts'
        WHEN 'NULL_CONSTRAINT' THEN '💡 Handle NULL values in data'
        WHEN 'DIVISION_BY_ZERO' THEN '💡 Add NULL/zero checks'
        WHEN 'TYPE_CONVERSION' THEN '💡 Review data types and conversions'
        ELSE '💡 Review error details and query logic'
    END AS recommendation,
    query_text_preview
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUILD_FAILURE_DETAILS
ORDER BY 
    CASE recency_severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    failure_time DESC
LIMIT 50;
```

**Additional Error Categories:**
- `CONSTRAINT_VIOLATION` - Primary key/unique constraint violations
- `NULL_CONSTRAINT` - NOT NULL constraint violations
- `DIVISION_BY_ZERO` - Mathematical errors
- `TYPE_CONVERSION` - Data type casting issues

---

### TILE 38: Top Recurring Error Patterns

**Type:** Table with occurrence frequency  
**Refresh:** Every hour

```sql
-- Recurring Error Hot Spots
SELECT 
    error_category,
    error_code,
    LEFT(error_message, 200) AS error_pattern,
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT DATE(error_time)) AS days_affected,
    COUNT(DISTINCT user_name) AS users_affected,
    COUNT(DISTINCT schema_name) AS schemas_affected,
    MIN(error_time) AS first_seen,
    MAX(error_time) AS last_seen,
    DATEDIFF('hour', MIN(error_time), MAX(error_time)) AS duration_hours,
    -- Priority classification
    CASE 
        WHEN COUNT(*) > 50 THEN '🔴 CRITICAL - System-wide issue'
        WHEN COUNT(*) > 20 THEN '🟠 HIGH - Needs immediate attention'
        WHEN COUNT(*) > 5 THEN '🟡 MEDIUM - Monitor and address'
        ELSE '🟢 LOW - Isolated incidents'
    END AS priority,
    -- Pattern indicator
    CASE 
        WHEN COUNT(DISTINCT DATE(error_time)) = 1 THEN '⚡ SPIKE - Single day'
        WHEN COUNT(*) / NULLIF(COUNT(DISTINCT DATE(error_time)), 0) > 10 THEN '🔥 FREQUENT - Multiple per day'
        ELSE '📊 RECURRING - Spread over time'
    END AS pattern_type
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY error_category, error_code, LEFT(error_message, 200)
HAVING COUNT(*) > 2
ORDER BY occurrence_count DESC, days_affected DESC
LIMIT 25;
```

---

### TILE 39: Error Timeline - Hourly Heatmap

**Type:** Heatmap (Date x Hour)  
**Refresh:** Every hour

```sql
-- Hourly Error Distribution Pattern
SELECT 
    DATE(error_time) AS error_date,
    HOUR(error_time) AS error_hour,
    COUNT(*) AS error_count,
    COUNT(DISTINCT user_name) AS unique_users,
    COUNT(DISTINCT error_category) AS error_variety,
    LISTAGG(DISTINCT error_category, ', ') WITHIN GROUP (ORDER BY error_category) AS error_types,
    -- Peak detection
    CASE 
        WHEN COUNT(*) > 50 THEN '🔴 PEAK'
        WHEN COUNT(*) > 20 THEN '🟠 HIGH'
        WHEN COUNT(*) > 10 THEN '🟡 ELEVATED'
        WHEN COUNT(*) > 5 THEN '🟢 MODERATE'
        ELSE '⚪ LOW'
    END AS intensity,
    -- Time of day classification
    CASE 
        WHEN HOUR(error_time) BETWEEN 0 AND 5 THEN '🌙 OFF-HOURS'
        WHEN HOUR(error_time) BETWEEN 6 AND 8 THEN '🌅 EARLY MORNING'
        WHEN HOUR(error_time) BETWEEN 9 AND 17 THEN '☀️ BUSINESS HOURS'
        WHEN HOUR(error_time) BETWEEN 18 AND 21 THEN '🌆 EVENING'
        ELSE '🌃 LATE NIGHT'
    END AS time_period
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY error_date, error_hour
ORDER BY error_date DESC, error_hour DESC;
```

**Insights:** Identify peak error times for:
- Scheduled job issues
- Concurrent workload conflicts
- Resource contention windows

---

### TILE 40: Error Analysis by User

**Type:** Table with user-level metrics  
**Refresh:** Daily

```sql
-- User Error Profile Analysis
SELECT 
    user_name,
    COUNT(*) AS total_errors,
    COUNT(DISTINCT error_category) AS error_variety,
    COUNT(DISTINCT schema_name) AS schemas_accessed,
    LISTAGG(DISTINCT error_category, ', ') WITHIN GROUP (ORDER BY error_category) AS error_types,
    MIN(error_time) AS first_error,
    MAX(error_time) AS last_error,
    DATEDIFF('day', MIN(error_time), MAX(error_time)) AS error_span_days,
    ROUND(COUNT(*) * 1.0 / NULLIF(DATEDIFF('day', MIN(error_time), MAX(error_time)), 0), 1) AS avg_errors_per_day,
    -- User status assessment
    CASE 
        WHEN COUNT(*) > 100 THEN '🔴 HIGH - Review access/training'
        WHEN COUNT(*) > 50 THEN '🟠 ELEVATED - Monitor activity'
        WHEN COUNT(*) > 20 THEN '🟡 MODERATE - Normal development'
        ELSE '🟢 LOW - Normal usage'
    END AS user_status,
    -- Recommendation
    CASE 
        WHEN COUNT(*) > 100 AND COUNT(DISTINCT error_category) = 1 THEN 'Training needed on specific error type'
        WHEN COUNT(*) > 100 THEN 'Review user permissions and access patterns'
        WHEN COUNT(DISTINCT error_category) > 5 THEN 'User may need general SQL training'
        ELSE 'No action needed'
    END AS recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY user_name
ORDER BY total_errors DESC;
```

---

### TILE 41: Error Analysis by Schema

**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- Schema-Level Error Distribution
SELECT 
    schema_name,
    COUNT(*) AS error_count,
    COUNT(DISTINCT error_category) AS error_variety,
    COUNT(DISTINCT query_type) AS query_type_variety,
    COUNT(DISTINCT user_name) AS affected_users,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total_errors,
    -- Most common error in this schema
    (SELECT error_category 
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG el2
     WHERE el2.schema_name = el.schema_name
       AND el2.error_time >= DATEADD('day', -7, CURRENT_DATE())
     GROUP BY error_category
     ORDER BY COUNT(*) DESC
     LIMIT 1) AS most_common_error,
    -- Schema health indicator
    CASE 
        WHEN COUNT(*) > 100 THEN '🔴 CRITICAL - Schema has major issues'
        WHEN COUNT(*) > 50 THEN '🟠 HIGH - Review schema objects'
        WHEN COUNT(*) > 20 THEN '🟡 MODERATE - Monitor'
        ELSE '🟢 HEALTHY'
    END AS schema_health
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG el
WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY schema_name
ORDER BY error_count DESC;
```

---

### TILE 42: Error-Free Days Tracker

**Type:** Calendar heatmap or table  
**Refresh:** Daily

```sql
-- Error-Free Days Quality Metric
WITH daily_status AS (
    SELECT 
        DATE(error_time) AS error_date,
        COUNT(*) AS error_count,
        COUNT(DISTINCT error_category) AS error_variety
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
    WHERE error_time >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY error_date
),
all_dates AS (
    SELECT 
        DATEADD('day', SEQ4(), DATEADD('day', -30, CURRENT_DATE())) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 31))
)
SELECT 
    ad.date,
    DAYNAME(ad.date) AS day_of_week,
    COALESCE(ds.error_count, 0) AS error_count,
    COALESCE(ds.error_variety, 0) AS error_types,
    -- Day classification
    CASE 
        WHEN ds.error_count IS NULL OR ds.error_count = 0 THEN '✅ ERROR-FREE'
        WHEN ds.error_count < 5 THEN '🟢 LOW (<5 errors)'
        WHEN ds.error_count < 20 THEN '🟡 MODERATE (5-20)'
        WHEN ds.error_count < 50 THEN '🟠 HIGH (20-50)'
        ELSE '🔴 CRITICAL (>50)'
    END AS day_status,
    -- Weekly quality score
    AVG(CASE WHEN ds2.error_count IS NULL THEN 100
             WHEN ds2.error_count = 0 THEN 100
             WHEN ds2.error_count < 5 THEN 90
             WHEN ds2.error_count < 20 THEN 70
             ELSE 40 
        END) OVER (ORDER BY ad.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quality_score_7day
FROM all_dates ad
LEFT JOIN daily_status ds ON ad.date = ds.error_date
LEFT JOIN daily_status ds2 ON ds2.error_date BETWEEN DATEADD('day', -6, ad.date) AND ad.date
ORDER BY ad.date DESC;
```

**Metrics:**
- Error-free days in last 30 days
- Average daily error count
- 7-day rolling quality score (0-100)

---

### TILE 43: Error Cost Impact Analysis

**Type:** Table with financial impact  
**Refresh:** Daily

```sql
-- Error Cost Impact (Wasted Compute)
WITH error_details AS (
    SELECT 
        DATE(e.error_time) AS error_date,
        e.warehouse_name,
        e.query_id,
        e.error_category,
        COALESCE(q.total_elapsed_time / 1000 / 3600, 0) AS execution_hours,
        COALESCE(q.warehouse_size, 'UNKNOWN') AS warehouse_size
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG e
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
        ON e.query_id = q.query_id
    WHERE e.error_time >= DATEADD('day', -30, CURRENT_DATE())
)
SELECT 
    error_date,
    error_category,
    COUNT(*) AS error_count,
    ROUND(SUM(execution_hours), 4) AS total_compute_hours,
    -- Estimate credits (assuming Medium warehouse = 4 credits/hour as baseline)
    ROUND(SUM(
        CASE warehouse_size
            WHEN 'X-Small' THEN execution_hours * 1
            WHEN 'Small' THEN execution_hours * 2
            WHEN 'Medium' THEN execution_hours * 4
            WHEN 'Large' THEN execution_hours * 8
            WHEN 'X-Large' THEN execution_hours * 16
            ELSE execution_hours * 4  -- Default to Medium
        END
    ), 4) AS estimated_credits_wasted,
    -- Cost at $3/credit
    ROUND(SUM(
        CASE warehouse_size
            WHEN 'X-Small' THEN execution_hours * 1
            WHEN 'Small' THEN execution_hours * 2
            WHEN 'Medium' THEN execution_hours * 4
            WHEN 'Large' THEN execution_hours * 8
            WHEN 'X-Large' THEN execution_hours * 16
            ELSE execution_hours * 4
        END
    ) * 3.0, 2) AS estimated_wasted_cost_usd,
    -- Impact indicator
    CASE 
        WHEN SUM(execution_hours) > 10 THEN '🔴 HIGH IMPACT (>10 hours)'
        WHEN SUM(execution_hours) > 1 THEN '🟡 MODERATE IMPACT (1-10 hours)'
        WHEN SUM(execution_hours) > 0.1 THEN '🟢 LOW IMPACT'
        ELSE '⚪ MINIMAL'
    END AS cost_impact
FROM error_details
GROUP BY error_date, error_category
HAVING SUM(execution_hours) > 0
ORDER BY estimated_wasted_cost_usd DESC;
```

**Business Value:** Track and reduce wasted compute costs from failed queries

---

## 🎯 QUICK ERROR DIAGNOSTIC QUERIES

### Quick Check 1: Current System Health

```sql
-- Real-Time Error Health Check
SELECT 
    -- Today's metrics
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG 
     WHERE DATE(error_time) = CURRENT_DATE()) AS errors_today,
    
    (SELECT ROUND(error_rate_pct, 2) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_TREND 
     WHERE date = CURRENT_DATE()) AS error_rate_today_pct,
    
    -- Last hour
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG 
     WHERE error_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())) AS errors_last_hour,
    
    -- Most common error right now
    (SELECT error_category FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG 
     WHERE DATE(error_time) = CURRENT_DATE()
     GROUP BY error_category ORDER BY COUNT(*) DESC LIMIT 1) AS top_error_today,
    
    -- Overall status
    CASE 
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG 
              WHERE error_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())) > 10 
        THEN '🔴 CRITICAL - High error rate'
        WHEN (SELECT ROUND(error_rate_pct, 2) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_TREND 
              WHERE date = CURRENT_DATE()) > 5 
        THEN '🟡 WARNING - Elevated errors'
        ELSE '✅ HEALTHY'
    END AS system_status;
```

---

### Quick Check 2: Latest 10 Errors

```sql
-- Most Recent Errors
SELECT 
    error_time,
    schema_name,
    error_category,
    LEFT(error_message, 100) AS error_summary,
    user_name,
    DATEDIFF('minute', error_time, CURRENT_TIMESTAMP()) AS minutes_ago
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG 
ORDER BY error_time DESC 
LIMIT 10;
```

---

### Quick Check 3: Error Count by Hour (Today)

```sql
-- Today's Hourly Error Distribution
SELECT 
    HOUR(error_time) AS hour,
    COUNT(*) AS error_count,
    LISTAGG(DISTINCT error_category, ', ') AS error_types
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE DATE(error_time) = CURRENT_DATE()
GROUP BY hour
ORDER BY hour;
```

---

### Quick Check 4: Error Summary Statistics

```sql
-- 7-Day Error Summary
SELECT 
    COUNT(*) AS total_errors_7d,
    COUNT(DISTINCT DATE(error_time)) AS days_with_errors,
    COUNT(DISTINCT error_category) AS unique_error_types,
    COUNT(DISTINCT user_name) AS affected_users,
    COUNT(DISTINCT schema_name) AS affected_schemas,
    ROUND(COUNT(*) * 1.0 / 7, 1) AS avg_errors_per_day,
    -- Most common error
    (SELECT error_category FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG 
     WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
     GROUP BY error_category ORDER BY COUNT(*) DESC LIMIT 1) AS most_common_error,
    -- Trend
    CASE 
        WHEN COUNT(CASE WHEN error_time >= DATEADD('day', -3, CURRENT_DATE()) THEN 1 END) >
             COUNT(CASE WHEN error_time BETWEEN DATEADD('day', -7, CURRENT_DATE()) 
                                           AND DATEADD('day', -4, CURRENT_DATE()) THEN 1 END)
        THEN '📈 INCREASING - Errors trending up'
        ELSE '📉 DECREASING or STABLE'
    END AS trend_indicator
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_time >= DATEADD('day', -7, CURRENT_DATE());
```

---

## 🔍 SCENARIO-SPECIFIC ERROR QUERIES

### Scenario 1: Post-Deployment Error Spike Detection

```sql
-- Detect Error Spikes After Deployments
WITH hourly_errors AS (
    SELECT 
        DATE_TRUNC('hour', error_time) AS error_hour,
        COUNT(*) AS error_count
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
    WHERE error_time >= DATEADD('day', -3, CURRENT_DATE())
    GROUP BY error_hour
),
baseline AS (
    SELECT AVG(error_count) AS avg_hourly_errors,
           STDDEV(error_count) AS stddev_errors
    FROM hourly_errors
)
SELECT 
    he.error_hour,
    he.error_count,
    ROUND(b.avg_hourly_errors, 1) AS baseline_avg,
    ROUND(b.stddev_errors, 1) AS baseline_stddev,
    ROUND((he.error_count - b.avg_hourly_errors) / NULLIF(b.stddev_errors, 0), 2) AS std_deviations,
    -- Spike detection
    CASE 
        WHEN he.error_count > b.avg_hourly_errors + (3 * b.stddev_errors) 
        THEN '🔴 CRITICAL SPIKE (>3σ)'
        WHEN he.error_count > b.avg_hourly_errors + (2 * b.stddev_errors) 
        THEN '🟠 SIGNIFICANT SPIKE (>2σ)'
        WHEN he.error_count > b.avg_hourly_errors + b.stddev_errors 
        THEN '🟡 ELEVATED (>1σ)'
        ELSE '🟢 NORMAL'
    END AS spike_status
FROM hourly_errors he
CROSS JOIN baseline b
WHERE he.error_count > b.avg_hourly_errors
ORDER BY he.error_hour DESC;
```

**Use Case:** Run after deploying new code to detect regression issues

---

### Scenario 2: Permission/Access Error Investigation

```sql
-- Access Denied & Permission Errors Analysis
SELECT 
    error_time,
    user_name,
    schema_name,
    error_code,
    error_message,
    LEFT(query_text_preview, 300) AS query_preview,
    -- Extract object from error message
    REGEXP_SUBSTR(error_message, 'table\\s+''([^'']+)''', 1, 1, 'ie', 1) AS denied_object,
    REGEXP_SUBSTR(error_message, 'schema\\s+''([^'']+)''', 1, 1, 'ie', 1) AS denied_schema,
    -- Action needed
    CASE 
        WHEN error_message ILIKE '%insufficient privileges%' THEN 'Grant role privileges'
        WHEN error_message ILIKE '%access denied%' THEN 'Check object ownership'
        WHEN error_message ILIKE '%does not exist or not authorized%' THEN 'Verify object exists and grant access'
        ELSE 'Review permissions'
    END AS action_needed
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_category = 'ACCESS_DENIED'
  AND error_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY error_time DESC;
```

**Use Case:** Security audit and access troubleshooting

---

### Scenario 3: Data Quality Issue Errors

```sql
-- Data Quality Related Errors
SELECT 
    failure_time,
    schema_name,
    affected_object,
    error_category,
    error_message,
    -- Extract specifics
    CASE 
        WHEN error_message ILIKE '%duplicate%' THEN 
            REGEXP_SUBSTR(error_message, 'key\\s+\\(([^)]+)\\)', 1, 1, 'ie', 1)
        WHEN error_message ILIKE '%null%not%null%' THEN 
            REGEXP_SUBSTR(error_message, 'column\\s+''([^'']+)''', 1, 1, 'ie', 1)
        WHEN error_message ILIKE '%division%zero%' THEN 'Division by zero in calculation'
        ELSE NULL
    END AS data_issue_detail,
    -- Root cause category
    CASE error_category
        WHEN 'CONSTRAINT_VIOLATION' THEN '🔴 DATA INTEGRITY - Duplicates/PKs'
        WHEN 'NULL_CONSTRAINT' THEN '🟠 DATA COMPLETENESS - Missing values'
        WHEN 'DIVISION_BY_ZERO' THEN '🟡 DATA VALIDITY - Invalid calculations'
        WHEN 'TYPE_CONVERSION' THEN '🟣 DATA FORMAT - Type mismatches'
        ELSE '⚪ OTHER'
    END AS root_cause,
    query_text_preview
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUILD_FAILURE_DETAILS
WHERE error_category IN ('CONSTRAINT_VIOLATION', 'NULL_CONSTRAINT', 
                         'DIVISION_BY_ZERO', 'TYPE_CONVERSION')
  AND failure_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY failure_time DESC;
```

**Use Case:** Identify data quality issues causing pipeline failures

---

### Scenario 4: Performance-Related Timeout Errors

```sql
-- Timeout & Resource Limit Analysis
SELECT 
    DATE(error_time) AS error_date,
    warehouse_name,
    schema_name,
    query_type,
    COUNT(*) AS timeout_count,
    AVG(DATEDIFF('second', 
        (SELECT start_time FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh 
         WHERE qh.query_id = el.query_id), error_time)) AS avg_runtime_seconds,
    -- Affected tables
    LISTAGG(DISTINCT 
        REGEXP_SUBSTR(query_text_preview, 'FROM\\s+([\\w_]+\\.[\\w_]+)', 1, 1, 'ie', 1), 
        ', ') AS affected_tables,
    -- Recommendation
    CASE 
        WHEN warehouse_name LIKE '%X-Small%' OR warehouse_name LIKE '%SMALL%' 
        THEN '💡 Consider larger warehouse for complex queries'
        WHEN COUNT(*) > 10 
        THEN '💡 Optimize queries or increase timeout settings'
        ELSE '💡 Review individual query patterns'
    END AS recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG el
WHERE error_category IN ('TIMEOUT', 'RESOURCE_LIMIT')
  AND error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY error_date, warehouse_name, schema_name, query_type
ORDER BY timeout_count DESC;
```

**Use Case:** Performance optimization and warehouse sizing

---

### Scenario 5: Syntax & Object Not Found Errors (Development Issues)

```sql
-- Development Error Patterns
SELECT 
    user_name,
    error_category,
    COUNT(*) AS error_count,
    -- Extract common patterns
    COUNT(CASE WHEN error_message ILIKE '%typo%' OR error_message ILIKE '%misspelled%' 
               THEN 1 END) AS typo_errors,
    COUNT(CASE WHEN error_message ILIKE '%does not exist%' 
               THEN 1 END) AS missing_object_errors,
    COUNT(CASE WHEN error_message ILIKE '%unexpected%' OR error_message ILIKE '%expected%' 
               THEN 1 END) AS syntax_errors,
    -- Most common specific error
    (SELECT LEFT(error_message, 100)
     FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG el2
     WHERE el2.user_name = el.user_name
       AND el2.error_category = el.error_category
       AND el2.error_time >= DATEADD('day', -7, CURRENT_DATE())
     GROUP BY LEFT(error_message, 100)
     ORDER BY COUNT(*) DESC
     LIMIT 1) AS most_common_error,
    -- Training recommendation
    CASE 
        WHEN COUNT(*) > 50 AND error_category = 'SYNTAX_ERROR' 
        THEN '📚 SQL syntax training recommended'
        WHEN COUNT(*) > 50 AND error_category = 'OBJECT_NOT_FOUND' 
        THEN '📚 Schema structure training recommended'
        WHEN COUNT(*) > 50 AND error_category = 'INVALID_IDENTIFIER' 
        THEN '📚 Naming conventions training recommended'
        ELSE '✅ Normal development learning curve'
    END AS training_recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG el
WHERE error_category IN ('SYNTAX_ERROR', 'OBJECT_NOT_FOUND', 'INVALID_IDENTIFIER')
  AND error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY user_name, error_category
HAVING COUNT(*) > 5
ORDER BY error_count DESC;
```

**Use Case:** Developer training and onboarding support

---

## 📊 ERROR DASHBOARD RECOMMENDED LAYOUT

### **Dashboard Tab: "Error Monitoring & Diagnostics"**

**Row 1: Executive Error Summary**
- Tile 34: Error Log (scrollable table)
- Quick Check 1: Current System Health (scorecard)
- Tile 35: Error Trend (dual-axis line chart)

**Row 2: Error Distribution & Analysis**
- Tile 36: Error Breakdown by Category (pie chart)
- Tile 38: Top Recurring Patterns (table)
- Tile 39: Hourly Heatmap (heatmap visualization)

**Row 3: Build Failures & Impact**
- Tile 37: Build Failure Details (table with recommendations)
- Tile 43: Error Cost Impact (table with $ values)
- Tile 42: Error-Free Days (calendar heatmap)

**Row 4: User & Schema Analysis**
- Tile 40: Error by User (horizontal bar chart)
- Tile 41: Error by Schema (horizontal bar chart)
- Quick Check 4: Summary Statistics (scorecard grid)

**Row 5: Scenario-Specific Deep Dives**
- Scenario 1: Deployment Spike Detection (run on-demand)
- Scenario 2: Permission Issues (filtered table)
- Scenario 3: Data Quality Errors (filtered table)

---

## 📈 ERROR METRICS SUMMARY

| Metric | Source | Purpose |
|--------|--------|---------|
| **Error Rate %** | `O2C_ENH_ERROR_TREND` | Overall system health |
| **Error Count** | `O2C_ENH_ERROR_LOG` | Volume tracking |
| **Error Categories** | `O2C_ENH_ERROR_LOG` | Root cause distribution |
| **Recurring Patterns** | Aggregated `ERROR_LOG` | Systemic issue identification |
| **Error-Free Days** | Date analysis | Quality trending |
| **Cost Impact** | `ERROR_LOG` + `QUERY_HISTORY` | Financial impact |
| **User Error Rate** | By user aggregation | Training needs |
| **Schema Error Rate** | By schema aggregation | Code quality by area |

---

## ✅ You're All Set!

**This file now contains 43 dashboard tiles + 9 diagnostic queries + 5 scenario-specific analyses.**

### Total Coverage:
1. ✅ **33 Core Monitoring Tiles** (Tiles 1-33)
2. ✅ **10 Error Analysis Tiles** (Tiles 34-43)
3. ✅ **4 Quick Diagnostic Queries** (Real-time health checks)
4. ✅ **5 Scenario-Specific Queries** (Deep-dive investigations)

### Next Steps:
1. ✅ Run `O2C_MONITORING_COMPLETE_SETUP.sql` to create all foundational views
2. ✅ Error monitoring views are already included in setup (Views 12-14)
3. ✅ Use this file to set up your comprehensive dashboard
4. ✅ Configure refresh schedules:
   - Error tiles: Every 5-15 minutes
   - Trend analysis: Hourly
   - User/Schema analysis: Daily
5. ✅ Bookmark Quick Check queries for instant diagnostics

**All your observability AND error analysis requirements are now covered in one place!** 🎉

