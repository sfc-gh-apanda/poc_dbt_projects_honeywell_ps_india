-- ============================================================================
-- O2C PROJECT MONITORING SETUP - SNOWFLAKE NATIVE DBT COMPATIBLE
-- ============================================================================
-- Purpose: Complete monitoring system for O2C dbt project
-- Uses: SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (works with Snowflake Native DBT)
-- Idempotent: YES - Can be run multiple times safely
-- ============================================================================

-- WHY O2C-SPECIFIC:
-- Filters for O2C schemas: O2C_STAGING, O2C_CORE, O2C_DIMENSIONS, O2C_AGGREGATES
-- Adds O2C source table freshness monitoring
-- Provides O2C-specific business metrics
-- ============================================================================

-- PREREQUISITES:
-- ✅ 1. O2C dbt project has run at least once
-- ✅ 2. SNOWFLAKE.ACCOUNT_USAGE access granted
-- ✅ 3. Run O2C_WAREHOUSE_CONFIG_SETUP.sql first (optional, for config table)
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Or your admin role
USE DATABASE EDW;

-- ============================================================================
-- STEP 1: CREATE O2C MONITORING SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS O2C_MONITORING
    COMMENT = 'O2C platform observability - Query History based monitoring';

USE SCHEMA O2C_MONITORING;

SELECT '✅ STEP 1 COMPLETE: O2C_MONITORING schema created' as status;

-- ============================================================================
-- STEP 2: O2C MODEL EXECUTION TRACKING
-- ============================================================================

-- View 2.1: O2C Model Executions (from Query History)
CREATE OR REPLACE VIEW O2C_MODEL_EXECUTIONS AS
SELECT 
    query_id,
    start_time as run_started_at,
    end_time,
    user_name,
    role_name,
    warehouse_name,
    database_name,
    schema_name,
    execution_status as status,
    total_elapsed_time / 1000.0 as total_node_runtime,
    rows_produced as rows_affected,
    bytes_scanned,
    compilation_time / 1000.0 as compilation_seconds,
    -- Extract model name from query
    COALESCE(
        REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1),
        schema_name || '.unknown_model'
    ) as node_id,
    SPLIT_PART(
        COALESCE(
            REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
            REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1)
        ), '.', -1
    ) as model_name,
    query_type,
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  -- O2C-specific schemas only
  AND schema_name IN ('O2C_STAGING', 'O2C_CORE', 'O2C_DIMENSIONS', 'O2C_AGGREGATES', 'O2C_SEMANTIC_VIEWS')
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND execution_status IN ('SUCCESS', 'FAIL')
  AND (
      query_text ILIKE '%create%or%replace%table%'
      OR query_text ILIKE '%create%or%replace%view%'
      OR query_text ILIKE '%insert%into%'
      OR query_text ILIKE '%merge%into%'
  );

COMMENT ON VIEW O2C_MODEL_EXECUTIONS IS 
    'O2C dbt model executions tracked from Query History';

-- View 2.2: O2C Test Executions (from Query History)
CREATE OR REPLACE VIEW O2C_TEST_EXECUTIONS AS
SELECT 
    query_id,
    start_time as run_started_at,
    end_time,
    user_name,
    execution_status as status,
    total_elapsed_time / 1000.0 as total_node_runtime,
    rows_produced,
    bytes_scanned,
    -- Extract test type from query
    CASE 
        WHEN query_text ILIKE '%not%null%' THEN 'not_null_test'
        WHEN query_text ILIKE '%unique%' THEN 'unique_test'
        WHEN query_text ILIKE '%relationships%' THEN 'relationships_test'
        WHEN query_text ILIKE '%accepted_values%' THEN 'accepted_values_test'
        WHEN query_text ILIKE '%dbt_expectations%' THEN 'expectations_test'
        WHEN query_text ILIKE '%dbt_utils%' THEN 'utils_test'
        ELSE 'generic_test'
    END as test_type,
    CASE 
        WHEN query_text ILIKE '%o2c%' THEN 'O2C Test'
        ELSE 'Test'
    END as node_id,
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND query_type = 'SELECT'
  -- Filter for O2C tests
  AND (query_text ILIKE '%o2c%' OR query_tag LIKE '%o2c%')
  AND (
      query_text ILIKE '%dbt_test%'
      OR (query_text ILIKE '%count(*)%' AND query_text ILIKE '%where%not%')
      OR query_text ILIKE '%dbt_utils%'
      OR query_text ILIKE '%dbt_expectations%'
  );

COMMENT ON VIEW O2C_TEST_EXECUTIONS IS 
    'O2C dbt test executions inferred from Query History';

SELECT '✅ STEP 2 COMPLETE: O2C execution tracking views created' as status;

-- ============================================================================
-- STEP 3: O2C SOURCE FRESHNESS MONITORING
-- ============================================================================

-- View 3.1: O2C Source Table Freshness
-- Note: Fact tables use CREATED_DATE, Dimension tables use LOAD_TS
CREATE OR REPLACE VIEW O2C_SOURCE_FRESHNESS AS
SELECT 
    'FACT_SALES_ORDERS' as source_table,
    'Transactions' as source_type,
    'CORP_TRAN' as schema_name,
    COUNT(*) as row_count,
    MAX(CREATED_DATE) as last_load_timestamp,
    DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) as hours_since_load,
    DATEDIFF('day', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) as days_since_load,
    CASE 
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END as freshness_status,
    CURRENT_TIMESTAMP() as checked_at
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS

UNION ALL

SELECT 
    'FACT_INVOICES',
    'Transactions',
    'CORP_TRAN',
    COUNT(*),
    MAX(CREATED_DATE),
    DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()),
    DATEDIFF('day', MAX(CREATED_DATE), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_INVOICES

UNION ALL

SELECT 
    'FACT_PAYMENTS',
    'Transactions',
    'CORP_TRAN',
    COUNT(*),
    MAX(CREATED_DATE),
    DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()),
    DATEDIFF('day', MAX(CREATED_DATE), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_PAYMENTS

UNION ALL

SELECT 
    'DIM_CUSTOMER',
    'Master Data',
    'CORP_MASTER',
    COUNT(*),
    MAX(LOAD_TS),
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    DATEDIFF('day', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 48 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 72 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_CUSTOMER

UNION ALL

SELECT 
    'DIM_PAYMENT_TERMS',
    'Master Data',
    'CORP_MASTER',
    COUNT(*),
    MAX(LOAD_TS),
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    DATEDIFF('day', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 72 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 168 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_PAYMENT_TERMS

UNION ALL

SELECT 
    'DIM_BANK_ACCOUNT',
    'Master Data',
    'CORP_MASTER',
    COUNT(*),
    MAX(LOAD_TS),
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    DATEDIFF('day', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 72 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 168 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_BANK_ACCOUNT

ORDER BY hours_since_load DESC;

COMMENT ON VIEW O2C_SOURCE_FRESHNESS IS 
    'O2C source table freshness monitoring';

-- View 3.2: O2C Model Layer Freshness
CREATE OR REPLACE VIEW O2C_MODEL_FRESHNESS AS
SELECT 
    'STG_ENRICHED_ORDERS' as model_name,
    'Staging' as layer,
    COUNT(*) as row_count,
    MAX(_dbt_loaded_at) as last_refresh,
    DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) as minutes_since_refresh,
    CASE 
        WHEN DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) <= 60 THEN '✅ Fresh'
        WHEN DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) <= 240 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END as freshness_status
FROM EDW.O2C_STAGING.STG_ENRICHED_ORDERS

UNION ALL

SELECT 
    'STG_ENRICHED_INVOICES',
    'Staging',
    COUNT(*),
    MAX(_dbt_loaded_at),
    DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) <= 60 THEN '✅ Fresh'
        WHEN DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) <= 240 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END
FROM EDW.O2C_STAGING.STG_ENRICHED_INVOICES

UNION ALL

SELECT 
    'STG_ENRICHED_PAYMENTS',
    'Staging',
    COUNT(*),
    MAX(_dbt_loaded_at),
    DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) <= 60 THEN '✅ Fresh'
        WHEN DATEDIFF('minute', MAX(_dbt_loaded_at), CURRENT_TIMESTAMP()) <= 240 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END
FROM EDW.O2C_STAGING.STG_ENRICHED_PAYMENTS

UNION ALL

SELECT 
    'DM_O2C_RECONCILIATION',
    'Core Mart',
    COUNT(*),
    MAX(loaded_at),
    DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) <= 60 THEN '✅ Fresh'
        WHEN DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) <= 240 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

UNION ALL

SELECT 
    'AGG_O2C_BY_CUSTOMER',
    'Aggregate',
    COUNT(*),
    MAX(loaded_at),
    DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) <= 60 THEN '✅ Fresh'
        WHEN DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) <= 240 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END
FROM EDW.O2C_AGGREGATES.AGG_O2C_BY_CUSTOMER

UNION ALL

SELECT 
    'AGG_O2C_BY_PERIOD',
    'Aggregate',
    COUNT(*),
    MAX(loaded_at),
    DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()),
    CASE 
        WHEN DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) <= 60 THEN '✅ Fresh'
        WHEN DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP()) <= 240 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END
FROM EDW.O2C_AGGREGATES.AGG_O2C_BY_PERIOD

ORDER BY minutes_since_refresh DESC;

COMMENT ON VIEW O2C_MODEL_FRESHNESS IS 
    'O2C dbt model freshness monitoring';

SELECT '✅ STEP 3 COMPLETE: O2C freshness monitoring views created' as status;

-- ============================================================================
-- STEP 4: O2C PERFORMANCE MONITORING
-- ============================================================================

-- View 4.1: O2C Daily Execution Summary
CREATE OR REPLACE VIEW O2C_DAILY_EXECUTION_SUMMARY AS
SELECT 
    DATE_TRUNC('day', run_started_at) as execution_date,
    COUNT(DISTINCT model_name) as models_run,
    COUNT(DISTINCT CASE WHEN status = 'SUCCESS' THEN model_name END) as successful_models,
    COUNT(DISTINCT CASE WHEN status = 'FAIL' THEN model_name END) as failed_models,
    SUM(total_node_runtime) as total_execution_seconds,
    ROUND(AVG(total_node_runtime), 2) as avg_execution_seconds,
    ROUND(MAX(total_node_runtime), 2) as max_execution_seconds,
    ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate_pct
FROM O2C_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

COMMENT ON VIEW O2C_DAILY_EXECUTION_SUMMARY IS 
    'Daily summary of O2C dbt model executions';

-- View 4.2: O2C Model Performance Ranking
CREATE OR REPLACE VIEW O2C_MODEL_PERFORMANCE_RANKING AS
SELECT 
    model_name,
    node_id,
    schema_name,
    COUNT(*) as run_count,
    ROUND(AVG(total_node_runtime), 2) as avg_execution_seconds,
    ROUND(MAX(total_node_runtime), 2) as max_execution_seconds,
    ROUND(MIN(total_node_runtime), 2) as min_execution_seconds,
    ROUND(STDDEV(total_node_runtime), 2) as stddev_execution_seconds,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(SUM(total_node_runtime), 2) as total_seconds,
    CASE 
        WHEN AVG(total_node_runtime) > 300 THEN '🔴 CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN '🟡 SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN '🟢 MODERATE'
        ELSE '⚪ FAST'
    END as performance_tier
FROM O2C_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY 1, 2, 3
HAVING run_count > 0
ORDER BY avg_execution_seconds DESC;

COMMENT ON VIEW O2C_MODEL_PERFORMANCE_RANKING IS 
    'O2C model performance metrics for optimization';

-- View 4.3: O2C Slowest Models
CREATE OR REPLACE VIEW O2C_SLOWEST_MODELS AS
SELECT 
    model_name,
    schema_name,
    COUNT(*) as run_count,
    ROUND(AVG(total_node_runtime), 2) as avg_seconds,
    ROUND(MAX(total_node_runtime), 2) as max_seconds,
    ROUND(SUM(total_node_runtime), 2) as total_seconds,
    -- Estimated cost (assumes $2/hour warehouse)
    ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) as estimated_cost_usd,
    CASE 
        WHEN AVG(total_node_runtime) > 300 THEN '🔴 CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN '🟡 SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN '🟢 MODERATE'
        ELSE '⚪ FAST'
    END as performance_tier
FROM O2C_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY avg_seconds DESC
LIMIT 10;

COMMENT ON VIEW O2C_SLOWEST_MODELS IS 
    'Top 10 slowest O2C models for optimization priority';

SELECT '✅ STEP 4 COMPLETE: O2C performance monitoring views created' as status;

-- ============================================================================
-- STEP 5: O2C ALERT VIEWS
-- ============================================================================

-- Alert 5.1: O2C Critical Performance Issues
CREATE OR REPLACE VIEW O2C_ALERT_PERFORMANCE AS
WITH model_baseline AS (
    SELECT 
        model_name,
        AVG(total_node_runtime) as baseline_avg,
        STDDEV(total_node_runtime) as baseline_stddev
    FROM O2C_MODEL_EXECUTIONS
    WHERE run_started_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'SUCCESS'
    GROUP BY model_name
    HAVING COUNT(*) >= 2
),
recent_runs AS (
    SELECT 
        model_name,
        AVG(total_node_runtime) as recent_avg,
        MAX(total_node_runtime) as recent_max,
        COUNT(*) as recent_run_count
    FROM O2C_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
      AND status = 'SUCCESS'
    GROUP BY model_name
)
SELECT 
    r.model_name,
    ROUND(b.baseline_avg, 2) as baseline_seconds,
    ROUND(r.recent_avg, 2) as recent_avg_seconds,
    ROUND(r.recent_max, 2) as recent_max_seconds,
    r.recent_run_count,
    ROUND((r.recent_avg - b.baseline_avg), 2) as seconds_slower,
    ROUND((r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) * 100, 1) as percent_slower,
    CASE 
        WHEN r.recent_avg > 300 AND (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 0.5 
            THEN 'CRITICAL'
        WHEN (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 1.0 
            THEN 'HIGH'
        WHEN (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 0.5 
            THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CURRENT_TIMESTAMP() as alert_time
FROM recent_runs r
JOIN model_baseline b ON r.model_name = b.model_name
WHERE r.recent_avg > b.baseline_avg + (2 * COALESCE(b.baseline_stddev, 0))
   OR r.recent_avg > 300
ORDER BY percent_slower DESC;

COMMENT ON VIEW O2C_ALERT_PERFORMANCE IS 
    'O2C models running significantly slower than baseline';

-- Alert 5.2: O2C Model Failures
CREATE OR REPLACE VIEW O2C_ALERT_MODEL_FAILURES AS
SELECT 
    run_started_at as failure_time,
    model_name,
    node_id,
    warehouse_name,
    total_node_runtime as execution_seconds,
    (SELECT COUNT(*) 
     FROM O2C_MODEL_EXECUTIONS m2 
     WHERE m2.model_name = m1.model_name 
       AND m2.status = 'FAIL'
       AND m2.run_started_at >= DATEADD(day, -7, CURRENT_DATE())
    ) as failures_last_7_days,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM O2C_MODEL_EXECUTIONS m2 
              WHERE m2.model_name = m1.model_name 
                AND m2.status = 'FAIL'
                AND m2.run_started_at >= DATEADD(day, -7, CURRENT_DATE())
             ) >= 3 THEN 'CRITICAL'
        ELSE 'HIGH'
    END as severity
FROM O2C_MODEL_EXECUTIONS m1
WHERE status = 'FAIL'
  AND run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY failures_last_7_days DESC, run_started_at DESC;

COMMENT ON VIEW O2C_ALERT_MODEL_FAILURES IS 
    'O2C model execution failures';

-- Alert 5.3: O2C Source Staleness Alerts
CREATE OR REPLACE VIEW O2C_ALERT_STALE_SOURCES AS
SELECT 
    source_table,
    source_type,
    last_load_timestamp,
    hours_since_load,
    days_since_load,
    freshness_status,
    CASE 
        WHEN hours_since_load > 72 THEN 'CRITICAL'
        WHEN hours_since_load > 48 THEN 'HIGH'
        WHEN hours_since_load > 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    'Source table stale: ' || source_table || ' last loaded ' || hours_since_load || ' hours ago' as alert_description,
    checked_at
FROM O2C_SOURCE_FRESHNESS
WHERE freshness_status != '✅ Fresh'
ORDER BY hours_since_load DESC;

COMMENT ON VIEW O2C_ALERT_STALE_SOURCES IS 
    'O2C source tables that are stale';

-- Alert 5.4: O2C Combined Alert Summary
CREATE OR REPLACE VIEW O2C_ALERT_SUMMARY AS
SELECT 
    CURRENT_TIMESTAMP() as snapshot_time,
    
    -- Performance Alerts
    (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') as critical_performance_issues,
    (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'HIGH') as high_performance_issues,
    
    -- Model Failures
    (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') as critical_model_failures,
    (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'HIGH') as high_model_failures,
    
    -- Source Staleness
    (SELECT COUNT(*) FROM O2C_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') as critical_stale_sources,
    (SELECT COUNT(*) FROM O2C_ALERT_STALE_SOURCES WHERE severity = 'HIGH') as high_stale_sources,
    
    -- Total Critical
    (
        (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') +
        (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') +
        (SELECT COUNT(*) FROM O2C_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL')
    ) as total_critical_alerts,
    
    -- Health Score (0-100)
    100 - (
        (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') * 15 +
        (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'HIGH') * 8 +
        (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') * 20 +
        (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'HIGH') * 10 +
        (SELECT COUNT(*) FROM O2C_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') * 10 +
        (SELECT COUNT(*) FROM O2C_ALERT_STALE_SOURCES WHERE severity = 'HIGH') * 5
    ) as health_score,
    
    -- Health Status
    CASE 
        WHEN (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') > 0 
            OR (SELECT COUNT(*) FROM O2C_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') > 0 
            THEN '🚨 CRITICAL'
        WHEN (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') > 0 
            THEN '⚠️ WARNING'
        WHEN (SELECT COUNT(*) FROM O2C_ALERT_PERFORMANCE WHERE severity = 'HIGH') > 0 
            OR (SELECT COUNT(*) FROM O2C_ALERT_MODEL_FAILURES WHERE severity = 'HIGH') > 0 
            THEN '🟡 ATTENTION'
        ELSE '✅ HEALTHY'
    END as health_status;

COMMENT ON VIEW O2C_ALERT_SUMMARY IS 
    'O2C platform health summary with alert counts';

SELECT '✅ STEP 5 COMPLETE: O2C alert views created' as status;

-- ============================================================================
-- STEP 6: O2C BUSINESS METRICS MONITORING
-- ============================================================================

-- View 6.1: O2C Business KPIs
CREATE OR REPLACE VIEW O2C_BUSINESS_KPIS AS
SELECT 
    -- Volume Metrics
    COUNT(DISTINCT order_key) as total_orders,
    COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) as invoiced_orders,
    COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) as paid_orders,
    
    -- Value Metrics
    SUM(order_amount) as total_order_value,
    SUM(invoice_amount) as total_invoice_value,
    SUM(payment_amount) as total_payment_value,
    SUM(outstanding_amount) as total_ar_outstanding,
    
    -- Performance Metrics
    ROUND(AVG(days_order_to_cash), 1) as avg_dso,
    ROUND(MEDIAN(days_order_to_cash), 1) as median_dso,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_order_to_cash), 1) as p90_dso,
    
    -- Conversion Rates
    ROUND(
        COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) * 100.0 /
        NULLIF(COUNT(DISTINCT order_key), 0), 1
    ) as billing_rate_pct,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END), 0), 1
    ) as collection_rate_pct,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN payment_timing = 'ON_TIME' THEN payment_key END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END), 0), 1
    ) as on_time_payment_pct,
    
    -- Snapshot Info
    CURRENT_TIMESTAMP() as snapshot_time
    
FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION
WHERE order_date >= DATEADD('month', -12, CURRENT_DATE());

COMMENT ON VIEW O2C_BUSINESS_KPIS IS 
    'O2C business KPIs for executive dashboard';

SELECT '✅ STEP 6 COMPLETE: O2C business metrics views created' as status;

-- ============================================================================
-- STEP 7: GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA O2C_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL VIEWS IN SCHEMA O2C_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL TABLES IN SCHEMA O2C_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA O2C_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA O2C_MONITORING TO ROLE DBT_O2C_DEVELOPER;

-- Also grant to prod role
GRANT USAGE ON SCHEMA O2C_MONITORING TO ROLE DBT_O2C_PROD;
GRANT SELECT ON ALL VIEWS IN SCHEMA O2C_MONITORING TO ROLE DBT_O2C_PROD;
GRANT SELECT ON ALL TABLES IN SCHEMA O2C_MONITORING TO ROLE DBT_O2C_PROD;

SELECT '✅ STEP 7 COMPLETE: Permissions granted' as status;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT '📊 VERIFICATION: Object Count Summary' as check_type;

SELECT 
    'Views' as object_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'O2C_MONITORING'

UNION ALL

SELECT 'Tables', COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'O2C_MONITORING'
  AND TABLE_TYPE = 'BASE TABLE';

-- List all views created
SELECT '📋 VERIFICATION: All O2C Monitoring Views' as check_type;
SELECT TABLE_NAME, COMMENT
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'O2C_MONITORING'
ORDER BY TABLE_NAME;

-- Show health summary
SELECT '🏥 VERIFICATION: Current O2C Health Status' as check_type;
SELECT * FROM O2C_ALERT_SUMMARY;

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================

SELECT '
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║     ✅ O2C MONITORING SETUP COMPLETE!                                    ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

📊 WHAT WAS CREATED:

✅ Schema: EDW.O2C_MONITORING

✅ Execution Tracking Views (2 views):
   - O2C_MODEL_EXECUTIONS
   - O2C_TEST_EXECUTIONS

✅ Freshness Monitoring Views (2 views):
   - O2C_SOURCE_FRESHNESS (6 source tables)
   - O2C_MODEL_FRESHNESS (6 models)

✅ Performance Monitoring Views (3 views):
   - O2C_DAILY_EXECUTION_SUMMARY
   - O2C_MODEL_PERFORMANCE_RANKING
   - O2C_SLOWEST_MODELS

✅ Alert Views (4 views):
   - O2C_ALERT_PERFORMANCE
   - O2C_ALERT_MODEL_FAILURES
   - O2C_ALERT_STALE_SOURCES
   - O2C_ALERT_SUMMARY

✅ Business Metrics (1 view):
   - O2C_BUSINESS_KPIS

✅ Permissions granted to DBT_O2C_DEVELOPER and DBT_O2C_PROD

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 NEXT STEPS:

1. Verify health status:
   SELECT * FROM EDW.O2C_MONITORING.O2C_ALERT_SUMMARY;

2. Check source freshness:
   SELECT * FROM EDW.O2C_MONITORING.O2C_SOURCE_FRESHNESS;

3. Check model freshness:
   SELECT * FROM EDW.O2C_MONITORING.O2C_MODEL_FRESHNESS;

4. View business KPIs:
   SELECT * FROM EDW.O2C_MONITORING.O2C_BUSINESS_KPIS;

5. Create Snowsight Dashboard:
   - See O2C_DASHBOARD_QUERIES.md for pre-built queries

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎉 YOUR O2C MONITORING SYSTEM IS NOW LIVE!

' as setup_complete;

