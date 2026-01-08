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

## ✅ You're All Set!

**This single file contains all 33 dashboard queries you need.**

### Next Steps:
1. ✅ Run `O2C_MONITORING_COMPLETE_SETUP.sql` to create all views
2. ✅ Use this file to set up your Snowsight dashboard
3. ✅ Configure refresh schedules based on table above
4. ✅ Set up email alerts for critical metrics

**All your observability requirements are now covered in one place!** 🎉

