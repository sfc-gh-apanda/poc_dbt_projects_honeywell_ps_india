-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED: ALL-IN-ONE MONITORING SETUP
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Complete monitoring setup - All views + All dashboard queries
-- Created: January 2025
-- Duration: ~5 minutes to create all views
-- Objects: 75+ monitoring views across 6 categories
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- EXECUTION INSTRUCTIONS:
-- 1. Run via SnowSQL: snowsql -f O2C_ALL_IN_ONE_MONITORING.sql
-- 2. Or copy sections A1-A6 into Snowsight worksheet
-- 3. Then use dashboard queries in PART B for Snowsight dashboard tiles
-- 4. Idempotent: Safe to re-run anytime
--
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE WAREHOUSE COMPUTE_WH;

-- ═══════════════════════════════════════════════════════════════════════════════
-- PART A: VIEW CREATION - Run these to create monitoring infrastructure
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- A.1: AUDIT FOUNDATION (REQUIRED - Run First!)
-- Creates: 3 tables + 1 view
-- Duration: ~30 seconds
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS EDW.O2C_AUDIT
    COMMENT = 'O2C Enhanced audit and monitoring foundation';

USE SCHEMA EDW.O2C_AUDIT;

-- Core audit tables
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

SELECT '✅ A.1 Complete: Audit foundation created (3 tables, 1 view)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- A.2: TELEMETRY VIEWS  
-- Creates: 4 views for data validation & tracking
-- Duration: ~10 seconds
-- ═══════════════════════════════════════════════════════════════════════════════

-- *** For full 75+ view creation, run these separate scripts in sequence: ***
-- 1. O2C_ENHANCED_TELEMETRY_SETUP.sql (4 views)
-- 2. O2C_ENHANCED_MONITORING_SETUP.sql (25 views)
-- 3. O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql (11 views)
-- 4. O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql (15 views)
-- 5. O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql (20 views)
--
-- These scripts are available in O2C/docs_o2c_enhanced/ folder
--
-- Quick command to run all:
-- cd O2C/docs_o2c_enhanced
-- snowsql -f O2C_ENHANCED_TELEMETRY_SETUP.sql
-- snowsql -f O2C_ENHANCED_MONITORING_SETUP.sql
-- snowsql -f O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql
-- snowsql -f O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql
-- snowsql -f O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql

SELECT '✅ To complete setup, run the 5 monitoring scripts listed above' AS next_step;
SELECT '📁 Location: O2C/docs_o2c_enhanced/*.sql' AS script_location;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION: Check what's been created
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═════════ SETUP VERIFICATION ═════════' AS section;

-- Count audit objects
SELECT 
    'Audit Tables' as object_type,
    COUNT(*) as count,
    CASE WHEN COUNT(*) >= 3 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM EDW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'O2C_AUDIT' AND TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 
    'Audit Views',
    COUNT(*),
    CASE WHEN COUNT(*) >= 5 THEN '✅ PASS' ELSE '⚠️ Run telemetry setup' END
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_AUDIT'
UNION ALL
SELECT 
    'Monitoring Views',
    COUNT(*),
    CASE WHEN COUNT(*) >= 70 THEN '✅ COMPLETE' ELSE '⚠️ Run monitoring setup scripts' END
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';

SELECT '═════════════════════════════════════' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- PART B: DASHBOARD QUERIES - Copy these to Snowsight dashboard tiles
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- After creating all views above, use the queries below as Snowsight dashboard tiles
-- Total: 25 comprehensive tiles covering all monitoring needs
--
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 1: Platform Health Overview - Executive Scorecard
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Single source of truth for complete platform status
-- Refresh: Every 5 minutes
-- Audience: Everyone
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
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
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 2: Daily Run Summary (Last 30 Days)
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track daily execution patterns and success rates
-- Type: Line chart with bars
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
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
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 3: Model Performance Dashboard
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Complete model performance analysis
-- Refresh: Every 4 hours
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
WITH latest_runs AS (
    SELECT 
        model_name, schema_name, run_count, avg_seconds, max_seconds,
        performance_tier, estimated_cost_usd, cost_per_execution,
        ROW_NUMBER() OVER (ORDER BY avg_seconds DESC) AS slowness_rank
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
)
SELECT 
    l.model_name,
    l.schema_name,
    l.run_count,
    l.avg_seconds,
    l.max_seconds,
    l.performance_tier,
    l.estimated_cost_usd,
    l.cost_per_execution,
    l.slowness_rank,
    CASE 
        WHEN l.performance_tier = '🔴 CRITICAL' THEN '🔴 SLOW'
        WHEN l.performance_tier = '🟢 FAST' THEN '🟢 HEALTHY'
        ELSE '🟡 NORMAL'
    END AS overall_health
FROM latest_runs l
ORDER BY l.avg_seconds DESC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 4: Test Execution Dashboard
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Complete test metrics and coverage
-- Refresh: Every 4 hours
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
WITH test_summary AS (
    SELECT 
        test_type, total_executions, passed, failed, pass_rate_pct, 
        avg_execution_sec, health_status
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_SUMMARY_BY_TYPE
),
test_coverage AS (
    SELECT 
        COUNT(*) AS total_models,
        SUM(CASE WHEN test_count > 0 THEN 1 ELSE 0 END) AS models_with_tests,
        ROUND(SUM(CASE WHEN test_count > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS coverage_pct
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE
),
recent_trend AS (
    SELECT pass_rate_pct AS latest_pass_rate, pass_rate_7day_avg
    FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_PASS_RATE_TREND
    ORDER BY test_date DESC LIMIT 1
)
SELECT 
    tc.total_models,
    tc.models_with_tests,
    tc.coverage_pct,
    rt.latest_pass_rate,
    rt.pass_rate_7day_avg,
    (SELECT SUM(total_executions) FROM test_summary) AS total_test_executions,
    (SELECT SUM(passed) FROM test_summary) AS total_passed,
    (SELECT SUM(failed) FROM test_summary) AS total_failed,
    CASE 
        WHEN tc.coverage_pct >= 95 AND rt.latest_pass_rate >= 95 THEN '🟢 EXCELLENT'
        WHEN tc.coverage_pct >= 80 AND rt.latest_pass_rate >= 90 THEN '🟡 GOOD'
        ELSE '🔴 NEEDS ATTENTION'
    END AS overall_test_health,
    CURRENT_TIMESTAMP() AS snapshot_time
FROM test_coverage tc CROSS JOIN recent_trend rt;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 5: Error Analysis Dashboard
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Complete error tracking and categorization
-- Refresh: Every 15 minutes
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    DATE(error_time) AS error_date,
    schema_name,
    error_category,
    error_code,
    COUNT(*) AS error_count,
    COUNT(DISTINCT LEFT(error_message, 100)) AS unique_error_types,
    MAX(error_time) AS last_occurrence,
    DATEDIFF('hour', MAX(error_time), CURRENT_TIMESTAMP()) AS hours_since_last,
    CASE 
        WHEN COUNT(*) >= 100 THEN '🔴 CRITICAL'
        WHEN COUNT(*) >= 50 THEN '🟠 HIGH'
        WHEN COUNT(*) >= 10 THEN '🟡 MEDIUM'
        ELSE '🟢 LOW'
    END AS severity
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ERROR_LOG
WHERE error_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY error_date, schema_name, error_category, error_code
ORDER BY error_date DESC, error_count DESC
LIMIT 100;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 6: Data Quality Metrics
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track data quality across all layers
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    layer,
    table_name,
    description,
    row_count,
    latest_record,
    checked_at,
    DATEDIFF('hour', latest_record, CURRENT_TIMESTAMP()) AS hours_since_latest,
    CASE 
        WHEN DATEDIFF('hour', latest_record, CURRENT_TIMESTAMP()) < 24 THEN '🟢 FRESH'
        WHEN DATEDIFF('hour', latest_record, CURRENT_TIMESTAMP()) < 48 THEN '🟡 RECENT'
        WHEN DATEDIFF('hour', latest_record, CURRENT_TIMESTAMP()) < 72 THEN '🟠 STALE'
        ELSE '🔴 VERY STALE'
    END AS freshness_status
FROM EDW.O2C_AUDIT.V_ROW_COUNT_TRACKING
ORDER BY layer, table_name;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 7: Data Flow Validation
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Validate data flow from source to staging (reconciliation)
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    entity,
    source_rows,
    staging_rows,
    row_variance,
    variance_pct,
    validation_status,
    validated_at
FROM EDW.O2C_AUDIT.V_DATA_FLOW_VALIDATION
ORDER BY ABS(variance_pct) DESC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 8: Cost Dashboard
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track Snowflake credit consumption and cost trends
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    date,
    total_credits_used,
    total_cost_usd,
    total_queries,
    avg_cost_per_query,
    cost_7day_moving_avg,
    cost_variance_from_avg_pct,
    CASE 
        WHEN cost_variance_from_avg_pct > 50 THEN '🔴 SPIKE'
        WHEN cost_variance_from_avg_pct > 25 THEN '🟠 ELEVATED'
        WHEN cost_variance_from_avg_pct < -25 THEN '🟢 LOW'
        ELSE '🟡 NORMAL'
    END AS cost_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY
WHERE date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY date DESC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 9: Performance Monitoring - Long Running Queries
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Identify slow queries needing optimization
-- Refresh: Every 4 hours
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    query_date,
    model_name,
    schema_name,
    execution_minutes,
    warehouse_name,
    rows_affected,
    performance_category,
    optimization_priority,
    CASE 
        WHEN execution_minutes > 60 THEN '🔴 CRITICAL'
        WHEN execution_minutes > 30 THEN '🟠 HIGH'
        WHEN execution_minutes > 15 THEN '🟡 MEDIUM'
        ELSE '🟢 MONITOR'
    END AS urgency
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_LONG_RUNNING_QUERIES
WHERE query_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY execution_minutes DESC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 10: Primary Key Validation
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Ensure PK uniqueness across all fact/dimension tables
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    model_name,
    primary_key_column,
    total_rows,
    distinct_pk_values,
    duplicate_count,
    validation_status,
    severity,
    checked_at,
    CASE 
        WHEN duplicate_count > 1000 THEN '🔴 CRITICAL'
        WHEN duplicate_count > 100 THEN '🟠 HIGH'
        WHEN duplicate_count > 0 THEN '🟡 LOW'
        ELSE '🟢 VALID'
    END AS health_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION
ORDER BY 
    CASE validation_status WHEN '❌ DUPLICATES FOUND' THEN 1 ELSE 2 END,
    duplicate_count DESC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 11: Foreign Key Validation
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Validate referential integrity across related tables
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    fact_table,
    fk_column,
    dimension_table,
    pk_column,
    total_fact_rows,
    orphaned_rows,
    orphan_pct,
    validation_status,
    severity,
    checked_at,
    CASE 
        WHEN orphan_pct > 10 THEN '🔴 CRITICAL'
        WHEN orphan_pct > 5 THEN '🟠 HIGH'
        WHEN orphan_pct > 0 THEN '🟡 LOW'
        ELSE '🟢 VALID'
    END AS health_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_FK_VALIDATION
ORDER BY 
    CASE validation_status WHEN '❌ ORPHANS FOUND' THEN 1 ELSE 2 END,
    orphan_pct DESC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 12: Schema Drift Detection
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track DDL changes and schema modifications
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    change_date,
    table_name,
    change_type,
    changed_by,
    columns_before,
    columns_after,
    column_count_diff,
    drift_severity,
    DATEDIFF('hour', change_date, CURRENT_TIMESTAMP()) AS hours_ago,
    CASE 
        WHEN drift_severity = 'HIGH' THEN '🔴 CRITICAL'
        WHEN drift_severity = 'MEDIUM' THEN '🟠 REVIEW'
        ELSE '🟡 INFO'
    END AS alert_level
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SCHEMA_DDL_CHANGES
WHERE change_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY change_date DESC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 13: dbt Test Coverage
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Identify models lacking test coverage
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    model_name,
    schema_name,
    test_count,
    passed_tests,
    failed_tests,
    pass_rate_pct,
    coverage_status,
    test_health,
    CASE 
        WHEN test_count = 0 AND schema_name LIKE '%CORE%' THEN '🔴 P0 - Core without tests'
        WHEN test_count = 0 THEN '🟠 P1 - No tests'
        WHEN failed_tests > 0 THEN '🔴 P0 - Failing tests'
        WHEN test_count < 3 THEN '🟡 P2 - Low coverage'
        ELSE '🟢 OK'
    END AS priority,
    last_model_run,
    DATEDIFF('day', last_model_run, CURRENT_DATE()) AS days_since_run
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE
ORDER BY 
    CASE priority 
        WHEN '🔴 P0 - Core without tests' THEN 1 
        WHEN '🔴 P0 - Failing tests' THEN 2 
        WHEN '🟠 P1 - No tests' THEN 3 
        ELSE 4 
    END,
    test_count ASC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 14: Warehouse Utilization
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Monitor warehouse resource usage and efficiency
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    hour,
    warehouse_name,
    query_count,
    total_credits,
    avg_execution_seconds,
    max_execution_seconds,
    utilization_pct,
    CASE 
        WHEN utilization_pct > 90 THEN '🔴 OVERUTILIZED'
        WHEN utilization_pct > 75 THEN '🟠 HIGH'
        WHEN utilization_pct < 20 THEN '🟡 UNDERUTILIZED'
        ELSE '🟢 OPTIMAL'
    END AS efficiency_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_WAREHOUSE_UTILIZATION
WHERE hour >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY hour DESC, total_credits DESC
LIMIT 100;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 15: Storage Usage & Growth
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track storage consumption and identify growth trends
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    table_schema,
    table_name,
    row_count,
    size_gb,
    size_pct_of_total,
    cumulative_pct,
    size_category,
    checked_at,
    CASE 
        WHEN size_gb > 100 THEN '🔴 VERY LARGE (>100 GB)'
        WHEN size_gb > 50 THEN '🟠 LARGE (>50 GB)'
        WHEN size_gb > 10 THEN '🟡 MEDIUM (>10 GB)'
        ELSE '🟢 SMALL (<10 GB)'
    END AS size_category_label
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_STORAGE_USAGE
ORDER BY size_gb DESC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 16: Active Alerts Summary
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Central alert dashboard for all active issues
-- Refresh: Every 5 minutes
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    alert_id,
    alert_name,
    alert_type,
    severity,
    LEFT(alert_message, 200) AS alert_preview,
    affected_objects,
    triggered_at,
    status,
    DATEDIFF('hour', triggered_at, CURRENT_TIMESTAMP()) AS hours_open,
    CASE 
        WHEN status = 'OPEN' AND severity = 'CRITICAL' AND DATEDIFF('hour', triggered_at, CURRENT_TIMESTAMP()) > 2 
        THEN '🔴 URGENT'
        WHEN status = 'OPEN' AND severity = 'CRITICAL' 
        THEN '🟠 CRITICAL'
        WHEN status = 'OPEN' 
        THEN '🟡 OPEN'
        WHEN status = 'ACKNOWLEDGED' 
        THEN '🔵 IN PROGRESS'
        ELSE '🟢 OK'
    END AS action_required
FROM EDW.O2C_AUDIT.V_ACTIVE_ALERTS
ORDER BY 
    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    triggered_at ASC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 17: Business KPIs Summary
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track key business metrics from O2C data
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    total_orders,
    ROUND(total_order_value / 1000, 1) AS order_value_k,
    ROUND(avg_order_value, 2) AS avg_order_value,
    total_invoices,
    ROUND(total_invoice_amount / 1000, 1) AS invoice_amount_k,
    total_payments,
    ROUND(total_payment_amount / 1000, 1) AS payment_amount_k,
    ROUND(total_ar_outstanding / 1000, 1) AS ar_outstanding_k,
    avg_dso,
    collection_efficiency_pct,
    snapshot_time,
    CASE 
        WHEN collection_efficiency_pct >= 95 THEN '🟢 EXCELLENT'
        WHEN collection_efficiency_pct >= 85 THEN '🟡 GOOD'
        WHEN collection_efficiency_pct >= 75 THEN '🟠 FAIR'
        ELSE '🔴 POOR'
    END AS collection_health
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_BUSINESS_KPIS;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 18: Null Rate Analysis (Data Completeness)
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track data completeness and identify columns with high null rates
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    model_name,
    column_name,
    total_rows,
    null_count,
    null_rate_pct,
    avg_null_rate_30d,
    null_rate_trend,
    CASE 
        WHEN null_rate_pct > 50 THEN '🔴 CRITICAL (>50% nulls)'
        WHEN null_rate_pct > 25 THEN '🟠 HIGH (>25% nulls)'
        WHEN null_rate_pct > 10 THEN '🟡 MEDIUM (>10% nulls)'
        WHEN null_rate_pct > 0 THEN '🟢 LOW (<10% nulls)'
        ELSE '✅ COMPLETE'
    END AS completeness_status,
    checked_at
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_NULL_RATE_TREND
WHERE null_rate_pct > 0
ORDER BY null_rate_pct DESC, model_name
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 19: Model Dependencies
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Visualize dbt model lineage and dependencies
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    model_name,
    schema_name,
    dependencies,
    dependency_count,
    dependency_depth,
    dependency_health,
    CASE 
        WHEN dependency_count > 5 THEN '🟠 COMPLEX (>5 deps)'
        WHEN dependency_count > 2 THEN '🟡 MODERATE (3-5 deps)'
        WHEN dependency_count > 0 THEN '🟢 SIMPLE (1-2 deps)'
        ELSE '✅ NO DEPENDENCIES'
    END AS complexity_level,
    last_run_date,
    DATEDIFF('day', last_run_date, CURRENT_DATE()) AS days_since_run
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_DEPENDENCIES
ORDER BY dependency_count DESC, model_name
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 20: Recurring Test Failures
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Identify tests that fail repeatedly (need attention)
-- Refresh: Every 4 hours
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    test_name,
    model_name,
    failure_count,
    last_failure_date,
    DATEDIFF('day', last_failure_date, CURRENT_DATE()) AS days_since_last_failure,
    CASE 
        WHEN failure_count >= 10 THEN '🔴 CHRONIC (10+ failures)'
        WHEN failure_count >= 5 THEN '🟠 RECURRING (5+ failures)'
        WHEN failure_count >= 2 THEN '🟡 MULTIPLE (2+ failures)'
        ELSE '🟢 ISOLATED'
    END AS failure_pattern,
    CASE 
        WHEN failure_count >= 10 THEN 'P0 - Fix immediately'
        WHEN failure_count >= 5 THEN 'P1 - Fix this week'
        WHEN failure_count >= 2 THEN 'P2 - Investigate'
        ELSE 'P3 - Monitor'
    END AS priority
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_TEST_RECURRING_FAILURES
ORDER BY failure_count DESC, last_failure_date DESC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 21: Queue Time Analysis
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Identify queries waiting in queue (concurrency issues)
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    hour,
    warehouse_name,
    queries_queued,
    avg_queue_seconds,
    max_queue_seconds,
    total_queue_minutes,
    CASE 
        WHEN avg_queue_seconds > 300 THEN '🔴 SEVERE (>5 min avg)'
        WHEN avg_queue_seconds > 120 THEN '🟠 HIGH (>2 min avg)'
        WHEN avg_queue_seconds > 60 THEN '🟡 MODERATE (>1 min avg)'
        ELSE '🟢 LOW (<1 min avg)'
    END AS queue_severity
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_QUEUE_TIME_BY_HOUR
WHERE hour >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY hour DESC, avg_queue_seconds DESC
LIMIT 100;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 22: Incremental Model Efficiency
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track incremental load performance (rows/second)
-- Refresh: Every 4 hours
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    model_name,
    schema_name,
    load_strategy,
    run_count,
    avg_rows_affected,
    avg_execution_seconds,
    rows_per_second,
    seconds_per_1k_rows,
    efficiency_status,
    CASE 
        WHEN efficiency_status LIKE '%INEFFICIENT%' THEN '🔴 OPTIMIZE'
        WHEN efficiency_status LIKE '%SLOW%' THEN '🟠 REVIEW'
        WHEN efficiency_status LIKE '%EFFICIENT%' THEN '🟢 HEALTHY'
        ELSE '🟡 NORMAL'
    END AS optimization_priority
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
ORDER BY 
    CASE efficiency_status 
        WHEN '🔴 INEFFICIENT (<100 rows/sec)' THEN 1 
        WHEN '🟠 SLOW (100-1K rows/sec)' THEN 2 
        ELSE 3 
    END,
    rows_per_second ASC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 23: Orphan/Stale Models
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Identify models not run recently or unused
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    model_name,
    schema_name,
    last_run_date,
    days_since_last_run,
    status,
    severity,
    CASE 
        WHEN days_since_last_run > 90 THEN '🔴 STALE (>90 days)'
        WHEN days_since_last_run > 30 THEN '🟠 OLD (>30 days)'
        WHEN days_since_last_run > 14 THEN '🟡 INACTIVE (>14 days)'
        ELSE '🟢 RECENT'
    END AS freshness_status,
    CASE 
        WHEN days_since_last_run > 90 THEN 'Consider deprecating'
        WHEN days_since_last_run > 30 THEN 'Verify usage'
        ELSE 'Monitor'
    END AS recommendation
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ORPHAN_STALE_MODELS
ORDER BY days_since_last_run DESC
LIMIT 50;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 24: Data Consistency Cross-Table Validation
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Validate related metrics across tables (e.g., invoices = orders)
-- Refresh: Hourly
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    validation_name,
    source_table,
    target_table,
    source_value,
    target_value,
    variance,
    variance_pct,
    validation_status,
    severity,
    checked_at,
    CASE 
        WHEN ABS(variance_pct) > 10 THEN '🔴 CRITICAL (>10% variance)'
        WHEN ABS(variance_pct) > 5 THEN '🟠 HIGH (>5% variance)'
        WHEN ABS(variance_pct) > 1 THEN '🟡 MINOR (>1% variance)'
        ELSE '🟢 MATCHED'
    END AS health_status
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_DATA_CONSISTENCY
ORDER BY 
    CASE validation_status WHEN '❌ MISMATCH' THEN 1 ELSE 2 END,
    ABS(variance_pct) DESC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- TILE 25: Monthly Cost Summary with MoM Comparison
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- Purpose: Track monthly cost trends and budget variance
-- Refresh: Daily
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
/*
SELECT 
    month,
    total_credits_used,
    total_cost_usd,
    total_queries,
    avg_cost_per_query,
    prev_month_cost_usd,
    mom_cost_change_usd,
    mom_cost_change_pct,
    CASE 
        WHEN mom_cost_change_pct > 25 THEN '🔴 HIGH INCREASE (>25%)'
        WHEN mom_cost_change_pct > 10 THEN '🟠 INCREASE (>10%)'
        WHEN mom_cost_change_pct < -10 THEN '🟢 DECREASE (<-10%)'
        ELSE '🟡 STABLE (±10%)'
    END AS cost_trend
FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY
ORDER BY month DESC;
*/

-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- END OF FILE
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
-- 
-- SUMMARY:
-- ✅ Part A: Creates audit foundation (3 tables + 1 view)
-- ✅ Part B: Points to 5 scripts that create 70+ monitoring views
-- ✅ Part B: Provides 25 dashboard queries covering:
--    - Platform health & executive scorecard
--    - Run metrics & execution tracking
--    - Model performance & optimization
--    - Error analysis & trends
--    - Test metrics & DQ coverage
--    - Data observability (freshness, reconciliation, completeness)
--    - Cost & performance monitoring
--    - Infrastructure (warehouse, storage, tasks)
--    - Data quality (PK/FK, schema drift, null rates, consistency)
--    - Alert management
--
-- NEXT STEPS:
-- 1. Run this script to create audit foundation
-- 2. Run the 5 referenced monitoring scripts
-- 3. Copy dashboard queries (TILE 1-25) to Snowsight
-- 4. Set refresh schedules per tile
-- 5. Configure alerts as needed
--
-- ═════════════════════════════════════════════════════════════════════════════════════════════════
