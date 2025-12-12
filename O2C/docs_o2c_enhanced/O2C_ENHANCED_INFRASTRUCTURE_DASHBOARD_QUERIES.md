# O2C Enhanced - Infrastructure Dashboard Queries

**Purpose:** Dashboard queries for Warehouse, Security, Storage, Task/Stream, and Concurrency monitoring  
**Platform:** Snowsight / Tableau / Power BI / Monte Carlo  
**Updated:** December 2024  
**Prerequisites:** `O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql` executed

---

## 📋 Table of Contents

1. [Warehouse & Resource Monitoring](#-warehouse--resource-monitoring)
2. [Security & Access Monitoring](#-security--access-monitoring)
3. [Storage Monitoring](#-storage-monitoring)
4. [Task & Stream Monitoring](#-task--stream-monitoring)
5. [Concurrency & Contention](#-concurrency--contention)
6. [Infrastructure Alerts Summary](#-infrastructure-alerts-summary)

---

## 🏭 Warehouse & Resource Monitoring

### **TILE 1: Warehouse Utilization Heatmap**

**Purpose:** Hourly utilization by warehouse  
**Type:** Heatmap or table  
**Refresh:** Every 30 minutes

```sql
-- ============================================================================
-- Warehouse Utilization by Hour (Last 7 Days)
-- ============================================================================
SELECT 
    DATE(hour_bucket) AS date,
    HOUR(hour_bucket) AS hour,
    warehouse_name,
    query_count,
    ROUND(compute_hours, 2) AS compute_hours,
    utilization_pct,
    utilization_status,
    sizing_recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_UTILIZATION
ORDER BY hour_bucket DESC;
```

---

### **TILE 2: Warehouse Credit Consumption Trend**

**Purpose:** Daily credit usage with 7-day average  
**Type:** Line chart with bars  
**Refresh:** Daily

```sql
-- ============================================================================
-- Daily Warehouse Credits (Last 30 Days)
-- ============================================================================
SELECT 
    usage_date,
    warehouse_name,
    warehouse_size,
    estimated_credits,
    ROUND(estimated_credits * 3.0, 2) AS estimated_cost_usd,
    query_count,
    gb_scanned,
    ROUND(credits_7day_avg, 2) AS credits_7day_avg
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_CREDITS
ORDER BY usage_date DESC, estimated_credits DESC;
```

---

### **TILE 3: Warehouse Concurrency Analysis**

**Purpose:** Peak concurrent queries by warehouse  
**Type:** Table  
**Refresh:** Every hour

```sql
-- ============================================================================
-- Warehouse Concurrency (Last 7 Days)
-- ============================================================================
SELECT 
    date,
    hour,
    warehouse_name,
    peak_concurrent,
    avg_concurrent,
    p95_concurrent,
    total_queued,
    avg_queue_seconds,
    concurrency_status,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_CONCURRENCY
WHERE date >= CURRENT_DATE() - 7
ORDER BY date DESC, hour DESC, peak_concurrent DESC;
```

---

### **TILE 4: Warehouse Scaling Events**

**Purpose:** Multi-cluster scaling history  
**Type:** Timeline/Table  
**Refresh:** Every hour

```sql
-- ============================================================================
-- Warehouse Scaling Events (Last 14 Days)
-- ============================================================================
SELECT 
    start_time,
    warehouse_name,
    cluster_number,
    event_display,
    reason_description,
    duration_seconds,
    event_state
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_SCALING
ORDER BY start_time DESC
LIMIT 50;
```

---

### **TILE 5: Warehouse Alerts**

**Purpose:** Current warehouse issues  
**Type:** Table with severity  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- Active Warehouse Alerts
-- ============================================================================
SELECT 
    alert_type,
    detected_at,
    warehouse_name,
    metric_value,
    alert_description,
    severity,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    detected_at DESC;
```

---

## 🔐 Security & Access Monitoring

### **TILE 6: Login Activity Summary**

**Purpose:** User login patterns  
**Type:** Table  
**Refresh:** Hourly

```sql
-- ============================================================================
-- Login Activity Summary (Last 30 Days)
-- ============================================================================
SELECT 
    login_date,
    user_name,
    client_type,
    auth_method,
    SUM(login_count) AS total_logins,
    SUM(successful_logins) AS successful,
    SUM(failed_logins) AS failed,
    ROUND(SUM(failed_logins) * 100.0 / NULLIF(SUM(login_count), 0), 1) AS failure_rate_pct,
    MAX(login_status) AS status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_LOGIN_HISTORY
GROUP BY login_date, user_name, client_type, auth_method
ORDER BY login_date DESC, failed DESC;
```

---

### **TILE 7: Data Access Patterns**

**Purpose:** Who accessed what data  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- Data Access Patterns (Last 30 Days)
-- ============================================================================
SELECT 
    access_date,
    user_name,
    role_name,
    schema_name,
    query_count,
    query_types_used,
    gb_scanned,
    access_pattern,
    activity_flag
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ACCESS_PATTERNS
WHERE access_pattern != '🟢 READ ONLY'  -- Focus on write operations
ORDER BY access_date DESC, query_count DESC;
```

---

### **TILE 8: Role Usage Analysis**

**Purpose:** Role activity and permissions  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- Role Usage Analysis (Last 30 Days)
-- ============================================================================
SELECT 
    role_name,
    permission_level,
    unique_users,
    active_days,
    total_queries,
    schemas_accessed,
    total_gb_scanned,
    last_activity,
    activity_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ROLE_USAGE
ORDER BY 
    CASE permission_level WHEN '🔴 ADMIN' THEN 1 WHEN '🟡 DEVELOPER' THEN 2 ELSE 3 END,
    total_queries DESC;
```

---

### **TILE 9: Failed Login Attempts**

**Purpose:** Security threat detection  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- Failed Login Attempts (Last 7 Days)
-- ============================================================================
SELECT 
    event_timestamp,
    user_name,
    client_ip,
    client_type,
    error_message,
    threat_indicator,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_FAILED_LOGINS
ORDER BY event_timestamp DESC
LIMIT 50;
```

---

### **TILE 10: Security Alerts**

**Purpose:** Security issues requiring attention  
**Type:** Table with severity  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- Security Alerts
-- ============================================================================
SELECT 
    alert_type,
    detected_at,
    affected_entity,
    metric_value,
    alert_description,
    severity
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    detected_at DESC;
```

---

## 💾 Storage Monitoring

### **TILE 11: Current Storage Usage**

**Purpose:** Storage by table with costs  
**Type:** Horizontal bar chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- Current Storage Usage by Table
-- ============================================================================
SELECT 
    schema_name,
    table_name,
    table_type,
    row_count,
    size_mb,
    size_gb,
    monthly_cost_usd,
    size_tier,
    rows_per_mb
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_USAGE
ORDER BY size_mb DESC
LIMIT 20;
```

---

### **TILE 12: Storage Growth Trend**

**Purpose:** Database growth over time  
**Type:** Line chart  
**Refresh:** Daily

```sql
-- ============================================================================
-- Storage Growth Trend (Last 90 Days)
-- ============================================================================
SELECT 
    usage_date,
    database_name,
    ROUND(total_gb, 2) AS total_gb,
    ROUND(daily_growth_gb, 4) AS daily_growth_gb,
    daily_growth_pct,
    ROUND(avg_daily_growth_7d_gb, 4) AS avg_daily_growth_7d_gb,
    est_monthly_cost_usd
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_GROWTH
ORDER BY usage_date DESC;
```

---

### **TILE 13: Table Size Rankings**

**Purpose:** Pareto analysis of storage  
**Type:** Table with cumulative %  
**Refresh:** Daily

```sql
-- ============================================================================
-- Table Size Rankings with Pareto
-- ============================================================================
SELECT 
    size_rank,
    schema_name || '.' || table_name AS table_path,
    row_count,
    size_gb,
    monthly_cost_usd,
    pct_of_total,
    cumulative_pct,
    size_tier
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TABLE_SIZES
ORDER BY size_rank
LIMIT 20;
```

---

### **TILE 14: Storage Forecast**

**Purpose:** Projected storage growth and costs  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- Storage Growth Forecast
-- ============================================================================
SELECT 
    database_name,
    current_gb,
    avg_daily_growth_gb,
    forecast_30d_gb,
    forecast_30d_cost_usd,
    forecast_90d_gb,
    forecast_90d_cost_usd,
    forecast_1yr_gb,
    forecast_1yr_monthly_cost_usd,
    growth_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_FORECAST;
```

---

### **TILE 15: Storage Alerts**

**Purpose:** Storage issues requiring attention  
**Type:** Table with severity  
**Refresh:** Daily

```sql
-- ============================================================================
-- Storage Alerts
-- ============================================================================
SELECT 
    alert_type,
    detected_at,
    affected_entity,
    metric_value,
    alert_description,
    severity,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STORAGE
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END;
```

---

## ⏰ Task & Stream Monitoring

### **TILE 16: Task Execution History**

**Purpose:** Recent task runs  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- Task Execution History (Last 14 Days)
-- ============================================================================
SELECT 
    scheduled_time,
    task_name,
    schema_name,
    status_display,
    execution_seconds,
    schedule_lag_seconds,
    duration_tier,
    error_message
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TASK_HISTORY
ORDER BY scheduled_time DESC
LIMIT 50;
```

---

### **TILE 17: Task Performance Summary**

**Purpose:** Task health and performance  
**Type:** Table with health indicators  
**Refresh:** Hourly

```sql
-- ============================================================================
-- Task Performance Summary (Last 7 Days)
-- ============================================================================
SELECT 
    task_name,
    schema_name,
    total_runs,
    successful_runs,
    failed_runs,
    success_rate_pct,
    avg_execution_seconds,
    max_execution_seconds,
    health_status,
    performance_status,
    last_run
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TASK_PERFORMANCE
ORDER BY 
    CASE health_status WHEN '🔴 UNHEALTHY' THEN 1 WHEN '🟡 DEGRADED' THEN 2 ELSE 3 END,
    failed_runs DESC;
```

---

### **TILE 18: Stream Status**

**Purpose:** Stream staleness monitoring  
**Type:** Table with status  
**Refresh:** Every 30 minutes

```sql
-- ============================================================================
-- Stream Lag Status
-- ============================================================================
SELECT 
    schema_name,
    stream_name,
    source_table,
    stream_type,
    hours_until_stale,
    stream_status,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STREAM_LAG
ORDER BY 
    CASE stream_status 
        WHEN '🔴 STALE - Data may be lost' THEN 1 
        WHEN '🟡 WARNING - Will stale soon' THEN 2 
        ELSE 3 
    END;
```

---

### **TILE 19: Task Dependencies**

**Purpose:** Task DAG overview  
**Type:** Table/Graph  
**Refresh:** Daily

```sql
-- ============================================================================
-- Task Dependencies
-- ============================================================================
SELECT 
    task_name,
    schema_name,
    task_type,
    predecessor_task,
    schedule,
    state,
    warehouse,
    last_altered
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TASK_DEPENDENCIES
ORDER BY task_type, task_name;
```

---

### **TILE 20: Task/Stream Alerts**

**Purpose:** Task and stream issues  
**Type:** Table with severity  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- Task and Stream Alerts
-- ============================================================================
SELECT 
    alert_type,
    detected_at,
    affected_entity,
    metric_value,
    alert_description,
    severity,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_TASKS
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    detected_at DESC;
```

---

## 🔄 Concurrency & Contention

### **TILE 21: Query Concurrency Overview**

**Purpose:** Peak and average concurrency  
**Type:** Table/Heatmap  
**Refresh:** Hourly

```sql
-- ============================================================================
-- Query Concurrency by Hour (Last 7 Days)
-- ============================================================================
SELECT 
    date,
    hour,
    warehouse_name,
    peak_concurrency,
    avg_concurrency,
    p95_concurrency,
    total_queued_minutes,
    avg_query_seconds,
    concurrency_status,
    resource_pressure
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_QUERY_CONCURRENCY
ORDER BY date DESC, hour DESC;
```

---

### **TILE 22: Blocked/Waiting Queries**

**Purpose:** Queries with significant wait times  
**Type:** Table  
**Refresh:** Every 30 minutes

```sql
-- ============================================================================
-- Blocked Queries (Last 7 Days)
-- ============================================================================
SELECT 
    start_time,
    query_id,
    warehouse_name,
    total_seconds,
    total_wait_seconds,
    wait_pct_of_total,
    wait_type,
    severity,
    query_type,
    LEFT(query_preview, 100) AS query_preview
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BLOCKED_QUERIES
ORDER BY total_wait_seconds DESC
LIMIT 25;
```

---

### **TILE 23: Lock Wait Analysis**

**Purpose:** Lock contention on tables  
**Type:** Table  
**Refresh:** Hourly

```sql
-- ============================================================================
-- Lock Waits (Last 7 Days)
-- ============================================================================
SELECT 
    start_time,
    affected_table,
    blocked_seconds,
    execution_seconds,
    lock_wait_pct,
    severity,
    user_name,
    LEFT(query_preview, 100) AS query_preview
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_LOCK_WAITS
ORDER BY blocked_seconds DESC;
```

---

### **TILE 24: Disk Spill Analysis**

**Purpose:** Memory pressure detection  
**Type:** Table  
**Refresh:** Daily

```sql
-- ============================================================================
-- Disk Spill Analysis (Last 14 Days)
-- ============================================================================
SELECT 
    query_date,
    warehouse_name,
    warehouse_size,
    total_queries,
    local_spill_queries,
    remote_spill_queries,
    local_spill_gb,
    remote_spill_gb,
    spill_rate_pct,
    spill_status,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SPILL_ANALYSIS
WHERE local_spill_gb > 0 OR remote_spill_gb > 0
ORDER BY query_date DESC, remote_spill_gb DESC;
```

---

### **TILE 25: Contention Alerts**

**Purpose:** All contention-related issues  
**Type:** Table with severity  
**Refresh:** Every 30 minutes

```sql
-- ============================================================================
-- Contention Alerts
-- ============================================================================
SELECT 
    alert_type,
    detected_at,
    affected_entity,
    metric_value,
    alert_description,
    severity,
    recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_CONTENTION
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
    detected_at DESC;
```

---

## 🚨 Infrastructure Alerts Summary

### **TILE 26: All Infrastructure Alerts**

**Purpose:** Unified view of all infrastructure alerts  
**Type:** Table with category  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- All Infrastructure Alerts (Unified)
-- ============================================================================
SELECT 'WAREHOUSE' AS category, alert_type, detected_at, affected_entity AS entity, 
       metric_value, alert_description, severity, recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE

UNION ALL

SELECT 'SECURITY', alert_type, detected_at, affected_entity, 
       metric_value, alert_description, severity, NULL
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY

UNION ALL

SELECT 'STORAGE', alert_type, detected_at, affected_entity, 
       metric_value, alert_description, severity, recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STORAGE

UNION ALL

SELECT 'TASK/STREAM', alert_type, detected_at, affected_entity, 
       metric_value, alert_description, severity, recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_TASKS

UNION ALL

SELECT 'CONTENTION', alert_type, detected_at, affected_entity, 
       metric_value, alert_description, severity, recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_CONTENTION

ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    detected_at DESC;
```

---

### **TILE 27: Infrastructure Health Scorecard**

**Purpose:** Overall infrastructure health  
**Type:** Scorecard  
**Refresh:** Every 15 minutes

```sql
-- ============================================================================
-- Infrastructure Health Scorecard
-- ============================================================================
SELECT 
    CURRENT_TIMESTAMP() AS snapshot_time,
    
    -- Warehouse Health
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS warehouse_issues,
    
    -- Security Concerns
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS security_issues,
    
    -- Storage Alerts
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STORAGE 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS storage_issues,
    
    -- Task/Stream Issues
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_TASKS 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS task_stream_issues,
    
    -- Contention Issues
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_CONTENTION 
     WHERE severity IN ('CRITICAL', 'HIGH')) AS contention_issues,
    
    -- Total Critical/High
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE WHERE severity = 'CRITICAL') +
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY WHERE severity = 'CRITICAL') +
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_STORAGE WHERE severity = 'CRITICAL') +
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_TASKS WHERE severity = 'CRITICAL') +
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_CONTENTION WHERE severity = 'CRITICAL')
    AS total_critical_alerts,
    
    -- Overall Health Status
    CASE 
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SECURITY WHERE severity = 'CRITICAL') > 0 
        THEN '🔴 CRITICAL - Security Issue'
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_TASKS WHERE severity = 'CRITICAL') > 0 
        THEN '🔴 CRITICAL - Task/Stream Issue'
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_CONTENTION WHERE severity = 'CRITICAL') > 0 
        THEN '🟠 WARNING - Contention Issue'
        WHEN (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_WAREHOUSE WHERE severity = 'HIGH') > 0 
        THEN '🟡 ATTENTION - Warehouse Issue'
        ELSE '🟢 HEALTHY'
    END AS infrastructure_status;
```

---

## ✅ Dashboard Tiles Summary

| # | Tile Name | Category | Type |
|---|-----------|----------|------|
| 1 | Warehouse Utilization Heatmap | Warehouse | Heatmap |
| 2 | Warehouse Credit Trend | Warehouse | Line Chart |
| 3 | Warehouse Concurrency | Warehouse | Table |
| 4 | Warehouse Scaling Events | Warehouse | Timeline |
| 5 | Warehouse Alerts | Warehouse | Table |
| 6 | Login Activity Summary | Security | Table |
| 7 | Data Access Patterns | Security | Table |
| 8 | Role Usage Analysis | Security | Table |
| 9 | Failed Login Attempts | Security | Table |
| 10 | Security Alerts | Security | Table |
| 11 | Current Storage Usage | Storage | Bar Chart |
| 12 | Storage Growth Trend | Storage | Line Chart |
| 13 | Table Size Rankings | Storage | Table |
| 14 | Storage Forecast | Storage | Table |
| 15 | Storage Alerts | Storage | Table |
| 16 | Task Execution History | Task/Stream | Table |
| 17 | Task Performance Summary | Task/Stream | Table |
| 18 | Stream Status | Task/Stream | Table |
| 19 | Task Dependencies | Task/Stream | Table |
| 20 | Task/Stream Alerts | Task/Stream | Table |
| 21 | Query Concurrency | Contention | Heatmap |
| 22 | Blocked Queries | Contention | Table |
| 23 | Lock Wait Analysis | Contention | Table |
| 24 | Disk Spill Analysis | Contention | Table |
| 25 | Contention Alerts | Contention | Table |
| 26 | All Infrastructure Alerts | Summary | Table |
| 27 | Infrastructure Health Scorecard | Summary | Scorecard |

---

## 📊 Refresh Schedule Summary

| Category | Recommended Refresh |
|----------|---------------------|
| Health Scorecard | Every 15 minutes |
| Warehouse Utilization | Every 30 minutes |
| Warehouse Credits | Daily |
| Security Alerts | Every 15 minutes |
| Login Activity | Hourly |
| Storage Usage | Daily |
| Storage Growth | Daily |
| Task Performance | Hourly |
| Stream Status | Every 30 minutes |
| Concurrency | Hourly |
| Contention Alerts | Every 30 minutes |

---

**Total Dashboard Tiles: 27**

Infrastructure monitoring ready! 🚀

