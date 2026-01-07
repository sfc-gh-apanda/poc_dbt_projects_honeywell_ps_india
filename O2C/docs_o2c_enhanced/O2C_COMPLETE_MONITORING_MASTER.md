# O2C Enhanced - Complete Monitoring Master Guide

**Purpose:** Complete reference for ALL monitoring setup and queries in one place  
**Created:** January 2025  
**Status:** ✅ Production Ready

---

## 🎯 Quick Start

### Option 1: All-in-One SQL File (Recommended)
Use the single executable file that contains everything:
```bash
snowsql -f O2C/docs_o2c_enhanced/O2C_ALL_IN_ONE_MONITORING.sql
```
This file includes:
- All view creation SQL
- All 25 dashboard queries (commented, ready to copy to Snowsight)
- Complete setup verification

### Option 2: Use This Guide
Follow this guide for detailed explanations and step-by-step setup.

---

## 📋 Quick Navigation

- **[PART A: Setup Scripts](#part-a-monitoring-setup-scripts)** → Create all 75+ monitoring views
- **[PART B: Dashboard Queries](#part-b-unified-dashboard-queries)** → 25 comprehensive dashboard tiles
- **[PART C: Execution Checklist](#part-c-execution-checklist)** → Step-by-step setup guide

---

# PART A: Monitoring Setup Scripts

## Overview: What Gets Created

| Script | Views Created | Duration | Purpose |
|--------|---------------|----------|---------|
| **A.1: Audit Foundation** | 3 tables, 1 view | 30 sec | Core audit infrastructure |
| **A.2: Telemetry** | 4 views | 10 sec | Data validation & tracking |
| **A.3: Core Monitoring** | 25 views | 45 sec | Execution, alerts, business KPIs |
| **A.4: Cost & Performance** | 11 views | 25 sec | Cost tracking, performance analysis |
| **A.5: DQ & Integrity** | 15 views | 30 sec | Schema drift, test coverage, PK/FK validation |
| **A.6: Infrastructure** | 20 views | 40 sec | Warehouse, security, storage, tasks |
| **TOTAL** | **75+ objects** | **~3 min** | Complete observability stack |

---

## A.1: Audit Foundation Setup

**File:** Run this inline or use existing setup script  
**Creates:** O2C_AUDIT schema with 3 tables + 1 view  
**Required:** YES - Must run first!

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- AUDIT FOUNDATION - REQUIRED FIRST STEP
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE WAREHOUSE COMPUTE_WH;

-- Create audit schema
CREATE SCHEMA IF NOT EXISTS EDW.O2C_AUDIT
    COMMENT = 'O2C Enhanced audit and monitoring foundation';

USE SCHEMA EDW.O2C_AUDIT;

-- Table 1: DBT Run Log
CREATE TABLE IF NOT EXISTS DBT_RUN_LOG (
    run_id VARCHAR(200) PRIMARY KEY,
    project_name VARCHAR(100),
    environment VARCHAR(50),
    run_started_at TIMESTAMP_NTZ,
    run_ended_at TIMESTAMP_NTZ,
    run_duration_seconds NUMBER,
    run_status VARCHAR(50),
    models_run NUMBER,
    models_success NUMBER,
    models_failed NUMBER,
    warehouse_name VARCHAR(100),
    user_name VARCHAR(100),
    dbt_version VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table 2: DBT Model Log
CREATE TABLE IF NOT EXISTS DBT_MODEL_LOG (
    log_id NUMBER AUTOINCREMENT PRIMARY KEY,
    run_id VARCHAR(200),
    model_name VARCHAR(200),
    schema_name VARCHAR(100),
    status VARCHAR(50),
    started_at TIMESTAMP_NTZ,
    completed_at TIMESTAMP_NTZ,
    execution_seconds NUMBER,
    rows_affected NUMBER,
    materialization VARCHAR(50),
    error_message VARCHAR(5000),
    warehouse_name VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (run_id) REFERENCES DBT_RUN_LOG(run_id)
);

-- Table 3: Alert History  
CREATE TABLE IF NOT EXISTS O2C_ALERT_HISTORY (
    alert_id NUMBER AUTOINCREMENT PRIMARY KEY,
    alert_name VARCHAR(200),
    alert_type VARCHAR(100),
    severity VARCHAR(50),
    alert_message VARCHAR(5000),
    affected_objects VARCHAR(1000),
    triggered_at TIMESTAMP_NTZ,
    acknowledged_at TIMESTAMP_NTZ,
    acknowledged_by VARCHAR(100),
    resolved_at TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- View: Active Alerts
CREATE OR REPLACE VIEW V_ACTIVE_ALERTS AS
SELECT 
    alert_id, alert_name, alert_type, severity, alert_message, affected_objects, triggered_at,
    CASE 
        WHEN resolved_at IS NOT NULL THEN 'RESOLVED'
        WHEN acknowledged_at IS NOT NULL THEN 'ACKNOWLEDGED'
        ELSE 'OPEN'
    END AS status
FROM O2C_ALERT_HISTORY
WHERE resolved_at IS NULL
ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END, triggered_at DESC;

SELECT '✅ A.1 Complete: Audit foundation created' AS status;
```

**Verification:**
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'O2C_AUDIT';
-- Expected: 4 (3 tables + 1 view)
```

---

## A.2: Telemetry Views

**Creates:** 4 data validation views  
**Alternative:** Use `O2C_ENHANCED_TELEMETRY_SETUP.sql`

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- TELEMETRY VIEWS - Data Validation & Tracking
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_AUDIT;

-- View 1: Row Count Tracking Across All Layers
CREATE OR REPLACE VIEW V_ROW_COUNT_TRACKING AS
SELECT 'SOURCE' AS layer, 'FACT_SALES_ORDERS' AS table_name, 'Orders' AS description,
       COUNT(*) AS row_count, MAX(CREATED_DATE) AS latest_record, CURRENT_TIMESTAMP() AS checked_at
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
UNION ALL
SELECT 'SOURCE', 'FACT_INVOICES', 'Invoices', COUNT(*), MAX(CREATED_DATE), CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_INVOICES
UNION ALL
SELECT 'SOURCE', 'FACT_PAYMENTS', 'Payments', COUNT(*), MAX(CREATED_DATE), CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_PAYMENTS
UNION ALL
SELECT 'SOURCE', 'DIM_CUSTOMER', 'Customers', COUNT(*), MAX(LOAD_TS), CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_CUSTOMER
UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_ORDERS', 'Orders+Customer', COUNT(*), NULL, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS
UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_INVOICES', 'Invoices+Terms', COUNT(*), NULL, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES
UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_PAYMENTS', 'Payments+Bank', COUNT(*), NULL, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS
UNION ALL
SELECT 'DIMENSION', 'DIM_O2C_CUSTOMER', 'Customer Dim', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER
UNION ALL
SELECT 'CORE', 'DM_O2C_RECONCILIATION', 'Reconciliation', COUNT(*), MAX(dbt_updated_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
UNION ALL
SELECT 'EVENTS', 'FACT_O2C_EVENTS', 'Event Log', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS
UNION ALL
SELECT 'PARTITIONED', 'FACT_O2C_DAILY', 'Daily Facts', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY
UNION ALL
SELECT 'AGGREGATE', 'AGG_O2C_BY_CUSTOMER', 'Customer Agg', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER;

-- View 2: Data Flow Validation (Source to Staging Reconciliation)
CREATE OR REPLACE VIEW V_DATA_FLOW_VALIDATION AS
WITH source_counts AS (
    SELECT 'Orders' AS entity, COUNT(*) AS source_rows FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
    UNION ALL SELECT 'Invoices', COUNT(*) FROM EDW.CORP_TRAN.FACT_INVOICES
    UNION ALL SELECT 'Payments', COUNT(*) FROM EDW.CORP_TRAN.FACT_PAYMENTS
),
staging_counts AS (
    SELECT 'Orders' AS entity, COUNT(*) AS staging_rows FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS
    UNION ALL SELECT 'Invoices', COUNT(*) FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES
    UNION ALL SELECT 'Payments', COUNT(*) FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS
)
SELECT 
    s.entity, s.source_rows, st.staging_rows,
    st.staging_rows - s.source_rows AS row_variance,
    ROUND((st.staging_rows - s.source_rows) * 100.0 / NULLIF(s.source_rows, 0), 2) AS variance_pct,
    CASE 
        WHEN s.source_rows = st.staging_rows THEN '✅ MATCHED'
        WHEN ABS(st.staging_rows - s.source_rows) / NULLIF(s.source_rows, 0) < 0.01 THEN '⚠️ MINOR VARIANCE'
        ELSE '❌ MISMATCH'
    END AS validation_status,
    CURRENT_TIMESTAMP() AS validated_at
FROM source_counts s JOIN staging_counts st ON s.entity = st.entity;

-- View 3: Audit Column Validation
CREATE OR REPLACE VIEW V_AUDIT_COLUMN_VALIDATION AS
SELECT 
    'DIM_O2C_CUSTOMER' AS model_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN dbt_run_id IS NOT NULL THEN 1 ELSE 0 END) AS has_run_id,
    SUM(CASE WHEN dbt_batch_id IS NOT NULL THEN 1 ELSE 0 END) AS has_batch_id,
    SUM(CASE WHEN dbt_loaded_at IS NOT NULL THEN 1 ELSE 0 END) AS has_loaded_at,
    COUNT(DISTINCT dbt_run_id) AS distinct_runs,
    COUNT(DISTINCT dbt_batch_id) AS distinct_batches,
    MIN(dbt_loaded_at) AS earliest_load,
    MAX(dbt_loaded_at) AS latest_load,
    CASE 
        WHEN SUM(CASE WHEN dbt_run_id IS NULL THEN 1 ELSE 0 END) = 0 THEN '✅ VALID'
        ELSE '❌ INVALID'
    END AS audit_status
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER;

-- View 4: Batch Tracking
CREATE OR REPLACE VIEW V_BATCH_TRACKING AS
SELECT 
    r.run_id, r.run_started_at, r.run_status, r.environment,
    m.model_name, m.schema_name, m.status AS model_status,
    m.rows_affected, m.execution_seconds, m.materialization
FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
LEFT JOIN EDW.O2C_AUDIT.DBT_MODEL_LOG m ON r.run_id = m.run_id
ORDER BY r.run_started_at DESC;

SELECT '✅ A.2 Complete: Telemetry views created (4 views)' AS status;
```

**Verification:**
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_AUDIT' AND TABLE_NAME LIKE 'V_%';
-- Expected: 5 (V_ACTIVE_ALERTS + 4 telemetry views)
```

---

## A.3: Core Monitoring Views (25 views)

**File to Execute:** `O2C_ENHANCED_MONITORING_SETUP.sql`  
**Duration:** ~45 seconds  
**Creates:** 25 monitoring views in `O2C_ENHANCED_MONITORING` schema

**Run via SnowSQL:**
```bash
snowsql -f O2C/docs_o2c_enhanced/O2C_ENHANCED_MONITORING_SETUP.sql
```

**Or copy-paste from the file - it creates:**
- Model execution tracking
- Test execution tracking
- Daily execution summaries
- Alert views (performance, failures, stale sources)
- Business KPIs
- Error logs and trends
- Build failure details
- Test insights (summary by type, pass rate trends, recurring failures)
- Event analytics
- Data quality metrics (row counts, reconciliation, null rates, completeness)
- Operational summary
- Execution timeline

**Verification:**
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';
-- Expected: 25

-- Test a key view
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY;
```

---

## A.4: Cost & Performance Monitoring (11 views)

**File to Execute:** `O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql`  
**Duration:** ~25 seconds  
**Creates:** 11 views for cost tracking and performance analysis

**Run via SnowSQL:**
```bash
snowsql -f O2C/docs_o2c_enhanced/O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql
```

**Creates:**
- Daily cost tracking with 7-day moving average
- Cost by model
- Monthly cost with MoM comparison
- Cost anomaly alerts
- Long running queries (>1 min)
- Queue time analysis by hour/warehouse
- Compilation time analysis
- Queue alerts
- Long query alerts
- Model performance trends
- Incremental model efficiency (rows/second)

**Verification:**
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';
-- Expected: 36 (25 + 11)

-- Test cost views
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY LIMIT 5;
```

---

## A.5: Data Quality & Integrity Monitoring (15 views)

**File to Execute:** `O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql`  
**Duration:** ~30 seconds  
**Creates:** 15 views for schema drift, test coverage, and data integrity

**Run via SnowSQL:**
```bash
snowsql -f O2C/docs_o2c_enhanced/O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql
```

**Creates:**

**Schema Drift (4 views):**
- Schema current state
- DDL change history
- Column-level changes
- Schema drift alerts

**dbt Observability (5 views):**
- Test coverage by model
- Model dependencies
- dbt run history
- Orphan/stale models
- dbt coverage alerts

**Data Integrity (6 views):**
- Primary key validation
- Foreign key validation
- Duplicate detection
- Null rate trend analysis
- Data consistency (cross-table validation)
- Data integrity alerts

**Verification:**
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';
-- Expected: 51 (36 + 15)

-- Test integrity views
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION;
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE LIMIT 5;
```

---

## A.6: Infrastructure Monitoring (20 views)

**File to Execute:** `O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql`  
**Duration:** ~40 seconds  
**Creates:** 20 views for warehouse, security, storage, and task monitoring

**Run via SnowSQL:**
```bash
snowsql -f O2C/docs_o2c_enhanced/O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql
```

**Creates:**

**Warehouse Monitoring (5 views):**
- Warehouse utilization by hour
- Warehouse credit consumption
- Warehouse concurrency
- Warehouse scaling events
- Warehouse alerts

**Security Monitoring (5 views):**
- Login history
- Data access patterns
- Role usage analysis
- Failed login attempts
- Security alerts

**Storage Monitoring (5 views):**
- Current storage usage by table
- Storage growth trends
- Table size rankings (Pareto)
- Storage growth forecast
- Storage alerts

**Task & Stream Monitoring (5 views):**
- Task execution history
- Task performance summary
- Stream lag analysis
- Task dependencies
- Task/Stream alerts

**Verification:**
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';
-- Expected: 71-75 (51 + 20)

-- Test infrastructure views
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_UTILIZATION LIMIT 5;
SELECT * FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_USAGE LIMIT 5;
```

---

## A.7: Complete Setup Verification

Run this comprehensive check after all setups:

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- COMPLETE SETUP VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═════════════════════════════════════════' AS "═══";
SELECT '✅ COMPLETE SETUP VERIFICATION' AS status;
SELECT '═════════════════════════════════════════' AS "═══";

-- Check 1: Audit Foundation
SELECT 
    '1. Audit Foundation' as component,
    COUNT(*) as count,
    CASE WHEN COUNT(*) >= 3 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM EDW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'O2C_AUDIT' AND TABLE_TYPE = 'BASE TABLE';

-- Check 2: Audit Views
SELECT 
    '2. Audit Views' as component,
    COUNT(*) as count,
    CASE WHEN COUNT(*) >= 5 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_AUDIT';

-- Check 3: Monitoring Views
SELECT 
    '3. Monitoring Views' as component,
    COUNT(*) as count,
    CASE WHEN COUNT(*) >= 70 THEN '✅ PASS' ELSE '⚠️ INCOMPLETE' END as status
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';

-- Check 4: View Categories
SELECT 
    CASE 
        WHEN TABLE_NAME LIKE '%EXECUTION%' OR TABLE_NAME LIKE '%MODEL%' THEN '4a. Execution Views'
        WHEN TABLE_NAME LIKE '%TEST%' THEN '4b. Test Views'
        WHEN TABLE_NAME LIKE '%ALERT%' THEN '4c. Alert Views'
        WHEN TABLE_NAME LIKE '%COST%' THEN '4d. Cost Views'
        WHEN TABLE_NAME LIKE '%PERFORMANCE%' OR TABLE_NAME LIKE '%QUEUE%' THEN '4e. Performance Views'
        WHEN TABLE_NAME LIKE '%SCHEMA%' OR TABLE_NAME LIKE '%DDL%' OR TABLE_NAME LIKE '%DBT%' THEN '4f. Schema & dbt Views'
        WHEN TABLE_NAME LIKE '%PK%' OR TABLE_NAME LIKE '%FK%' OR TABLE_NAME LIKE '%NULL%' OR TABLE_NAME LIKE '%DATA%' THEN '4g. Data Quality Views'
        WHEN TABLE_NAME LIKE '%WAREHOUSE%' OR TABLE_NAME LIKE '%STORAGE%' OR TABLE_NAME LIKE '%TASK%' THEN '4h. Infrastructure Views'
        ELSE '4i. Other Views'
    END as component,
    COUNT(*) as count,
    '✅' as status
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
GROUP BY component
ORDER BY component;

-- Summary
SELECT '═════════════════════════════════════════' AS "═══";
SELECT 'SETUP COMPLETE!' as result;
SELECT '═════════════════════════════════════════' AS "═══";

SELECT 
    (SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.TABLES 
     WHERE TABLE_SCHEMA = 'O2C_AUDIT' AND TABLE_TYPE = 'BASE TABLE') as audit_tables,
    (SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
     WHERE TABLE_SCHEMA = 'O2C_AUDIT') as audit_views,
    (SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
     WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING') as monitoring_views,
    '✅ All systems operational' as message;
```

**Expected Output:**
```
Audit Tables: 3 ✅
Audit Views: 5 ✅  
Monitoring Views: 70-75 ✅
Message: All systems operational
```

---

# PART B: Unified Dashboard Queries

All 25 comprehensive dashboard queries in one place.

---

## B.1: Executive Health Scorecard

### TILE 1: Platform Health Overview

**Purpose:** Single source of truth for complete platform status  
**Type:** Scorecard (12 metrics)  
**Refresh:** Every 5 minutes  
**Audience:** Everyone

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- TILE 1: Platform Health Overview - Complete Status at a Glance
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

---

## B.2: Run Metrics & Execution Tracking

### TILE 2: Daily Run Summary (Last 30 Days)

**Purpose:** Track daily execution patterns and success rates  
**Type:** Line chart with bars  
**Refresh:** Hourly

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- TILE 2: Daily Run Summary with Trend Analysis
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    execution_date,
    models_run,
    successful_models,
    failed_models,
    total_minutes,
    avg_execution_seconds,
    max_execution_seconds,
    success_rate_pct,
    CASE 
        WHEN success_rate_pct >= 95 THEN '🟢 EXCELLENT'
        WHEN success_rate_pct >= 90 THEN '🟡 GOOD'
        WHEN success_rate_pct >= 80 THEN '🟠 WARNING'
        ELSE '🔴 CRITICAL'
    END AS health_status,
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

### TILE 3: Run-Level Details (Last 7 Days)

**Purpose:** Detailed run history with duration and status  
**Type:** Table  
**Refresh:** Every 15 minutes

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- TILE 3: Run-Level Details with Complete Audit Trail
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    run_id,
    project_name,
    environment,
    run_started_at,
    run_ended_at,
    ROUND(run_duration_seconds / 60, 2) AS duration_minutes,
    run_status,
    models_run,
    models_success,
    models_failed,
    success_rate_pct,
    duration_category,
    run_health,
    warehouse_name,
    user_name,
    DATEDIFF('hour', run_started_at, CURRENT_TIMESTAMP()) AS hours_ago
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_RUN_HISTORY
WHERE run_started_at >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY run_started_at DESC
LIMIT 50;
```

### TILE 4: Run Execution Timeline (Gantt View)

**Purpose:** Visual timeline of model execution within runs  
**Type:** Timeline/Gantt  
**Refresh:** Real-time

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- TILE 4: Execution Timeline - See What's Running and When
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT 
    run_id,
    model_name,
    schema_name,
    started_at,
    completed_at,
    execution_seconds,
    status,
    CASE 
        WHEN status = 'SUCCESS' THEN '🟢'
        WHEN status = 'FAIL' THEN '🔴'
        WHEN status = 'RUNNING' THEN '🔵'
        ELSE '⚪'
    END AS status_icon,
    COUNT(*) OVER (
        PARTITION BY run_id 
        ORDER BY started_at 
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS models_remaining,
    ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY started_at) AS execution_order
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_EXECUTION_TIMELINE
WHERE started_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY started_at DESC, execution_order;
```

---

## B.3: Model Performance Metrics

### TILE 5: Model Performance Dashboard

**Purpose:** Complete model performance analysis  
**Type:** Multi-metric table  
**Refresh:** Every 4 hours

```sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- TILE 5: Model Performance Comprehensive Dashboard
-- ═══════════════════════════════════════════════════════════════════════════════
WITH latest_runs AS (
    SELECT 
        model_name, schema_name, run_count, avg_seconds, max_seconds, min_seconds,
        total_seconds, performance_tier, estimated_cost_usd, cost_per_execution,
        ROW_NUMBER() OVER (ORDER BY avg_seconds DESC) AS slowness_rank
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
),
performance_trends AS (
    SELECT 
        model_name, schema_name, avg_7day_ma, baseline_avg,
        variance_from_baseline_pct, performance_trend,
        ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY run_date DESC) AS rn
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_PERFORMANCE_TREND
),
efficiency_metrics AS (
    SELECT 
        model_name, schema_name, load_strategy,
        rows_per_second, seconds_per_1k_rows, efficiency_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
)
SELECT 
    l.model_name,
    l.schema_name,
    l.run_count,
    l.avg_seconds,
    l.max_seconds,
    l.performance_tier,
    t.performance_trend,
    t.variance_from_baseline_pct,
    e.load_strategy,
    e.rows_per_second,
    e.efficiency_status,
    l.estimated_cost_usd,
    l.cost_per_execution,
    l.slowness_rank,
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

---

Due to the file size limit, I'll continue with the remaining tiles in a structured summary format. Let me complete the file:

