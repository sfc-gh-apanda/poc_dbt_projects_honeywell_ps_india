-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - COMPREHENSIVE MONITORING SETUP
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Complete monitoring system for dbt_o2c_enhanced project
-- Uses: SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (Snowflake Native DBT compatible)
-- 
-- Views Created (25 total):
--   EXECUTION TRACKING:
--     1. O2C_ENH_MODEL_EXECUTIONS      - Model execution from Query History
--     2. O2C_ENH_TEST_EXECUTIONS       - Test execution from Query History
--     3. O2C_ENH_DAILY_EXECUTION_SUMMARY - Daily execution summary
--     4. O2C_ENH_SLOWEST_MODELS        - Performance bottlenecks
--   
--   FRESHNESS:
--     5. O2C_ENH_SOURCE_FRESHNESS      - Source table freshness
--     6. O2C_ENH_MODEL_FRESHNESS       - dbt model freshness
--   
--   ALERTS:
--     7. O2C_ENH_ALERT_PERFORMANCE     - Performance degradation alerts
--     8. O2C_ENH_ALERT_MODEL_FAILURES  - Model failure alerts
--     9. O2C_ENH_ALERT_STALE_SOURCES   - Stale source alerts
--    10. O2C_ENH_ALERT_SUMMARY         - Alert dashboard summary
--   
--   BUSINESS:
--    11. O2C_ENH_BUSINESS_KPIS         - Business metrics
--   
--   ERROR ANALYSIS:
--    12. O2C_ENH_ERROR_LOG             - Detailed error log
--    13. O2C_ENH_ERROR_TREND           - Error trend analysis
--   
--   BUILD FAILURE DETAILS:
--    14. O2C_ENH_BUILD_FAILURE_DETAILS - Extended build failure analysis
--   
--   TEST INSIGHTS:
--    15. O2C_ENH_TEST_SUMMARY_BY_TYPE  - Test summary by test type
--    16. O2C_ENH_TEST_PASS_RATE_TREND  - Test pass rate trend
--    17. O2C_ENH_RECURRING_TEST_FAILURES - Recurring test failures
--   
--   EVENT TABLE ANALYTICS:
--    18. O2C_ENH_EVENT_ANALYTICS       - Event table analytics
--    19. O2C_ENH_EVENT_TIMELINE        - Event timeline (30 days)
--   
--   DATA QUALITY:
--    20. O2C_ENH_ROW_COUNT_TRACKING    - Row counts across all layers
--    21. O2C_ENH_DATA_RECONCILIATION   - Source to staging reconciliation
--    22. O2C_ENH_NULL_RATE_ANALYSIS    - Null rate analysis
--    23. O2C_ENH_DATA_COMPLETENESS     - Data completeness scorecard
--   
--   OPERATIONAL:
--    24. O2C_ENH_EXECUTION_TIMELINE    - Execution timeline (Gantt-style)
--    25. O2C_ENH_OPERATIONAL_SUMMARY   - Comprehensive operational summary
-- 
-- Prerequisites:
--   - O2C_ENHANCED_AUDIT_SETUP.sql executed
--   - dbt_o2c_enhanced project has run at least once
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- Create monitoring schema if not exists
CREATE SCHEMA IF NOT EXISTS O2C_ENHANCED_MONITORING
    COMMENT = 'O2C Enhanced platform observability - Query History based monitoring';

USE SCHEMA O2C_ENHANCED_MONITORING;

SELECT '✅ STEP 1: O2C_ENHANCED_MONITORING schema ready' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 1: MODEL EXECUTIONS (from Query History)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_MODEL_EXECUTIONS AS
SELECT 
    query_id,
    start_time AS run_started_at,
    end_time,
    user_name,
    role_name,
    warehouse_name,
    database_name,
    schema_name,
    execution_status AS status,
    total_elapsed_time / 1000.0 AS total_node_runtime,
    rows_produced AS rows_affected,
    bytes_scanned,
    compilation_time / 1000.0 AS compilation_seconds,
    -- Extract model name from query
    COALESCE(
        REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1),
        schema_name || '.unknown_model'
    ) AS node_id,
    SPLIT_PART(
        COALESCE(
            REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
            REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1)
        ), '.', -1
    ) AS model_name,
    query_type,
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  -- O2C Enhanced schemas (flexible pattern matching)
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND execution_status IN ('SUCCESS', 'FAIL')
  AND (
      query_text ILIKE '%create%or%replace%table%'
      OR query_text ILIKE '%create%or%replace%view%'
      OR query_text ILIKE '%insert%into%'
      OR query_text ILIKE '%merge%into%'
      OR query_text ILIKE '%delete%from%'
  );

COMMENT ON VIEW O2C_ENH_MODEL_EXECUTIONS IS 
    'O2C Enhanced dbt model executions tracked from Query History';

SELECT '✅ VIEW 1 CREATED: O2C_ENH_MODEL_EXECUTIONS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 2: TEST EXECUTIONS (from Query History)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TEST_EXECUTIONS AS
SELECT 
    query_id,
    start_time AS run_started_at,
    end_time,
    user_name,
    execution_status AS status,
    total_elapsed_time / 1000.0 AS total_node_runtime,
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
    END AS test_type,
    'O2C_Enhanced Test' AS node_id,
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND query_type = 'SELECT'
  AND (query_text ILIKE '%o2c_enhanced%' OR query_tag LIKE '%o2c_enhanced%')
  AND schema_name LIKE 'O2C_ENHANCED_%'
  AND (
      query_text ILIKE '%dbt_test%'
      OR (query_text ILIKE '%count(*)%' AND query_text ILIKE '%where%not%')
      OR query_text ILIKE '%dbt_utils%'
      OR query_text ILIKE '%dbt_expectations%'
  );

COMMENT ON VIEW O2C_ENH_TEST_EXECUTIONS IS 
    'O2C Enhanced dbt test executions inferred from Query History';

SELECT '✅ VIEW 2 CREATED: O2C_ENH_TEST_EXECUTIONS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 3: DAILY EXECUTION SUMMARY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DAILY_EXECUTION_SUMMARY AS
SELECT 
    DATE(run_started_at) AS execution_date,
    COUNT(DISTINCT model_name) AS models_run,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_models,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_models,
    ROUND(SUM(total_node_runtime) / 60, 2) AS total_minutes,
    ROUND(AVG(total_node_runtime), 2) AS avg_execution_seconds,
    MAX(total_node_runtime) AS max_execution_seconds,
    SUM(rows_affected) AS total_rows_affected,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS success_rate_pct
FROM O2C_ENH_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY execution_date
ORDER BY execution_date DESC;

COMMENT ON VIEW O2C_ENH_DAILY_EXECUTION_SUMMARY IS 
    'Daily summary of O2C Enhanced dbt model executions';

SELECT '✅ VIEW 3 CREATED: O2C_ENH_DAILY_EXECUTION_SUMMARY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 4: SLOWEST MODELS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_SLOWEST_MODELS AS
SELECT 
    model_name,
    schema_name,
    COUNT(*) AS run_count,
    ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
    ROUND(MAX(total_node_runtime), 2) AS max_seconds,
    ROUND(MIN(total_node_runtime), 2) AS min_seconds,
    ROUND(SUM(total_node_runtime), 2) AS total_seconds,
    -- Estimated cost (assumes $2/hour warehouse)
    ROUND((SUM(total_node_runtime) / 3600) * 2.0, 2) AS estimated_cost_usd,
    CASE 
        WHEN AVG(total_node_runtime) > 300 THEN '🔴 CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN '🟡 SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN '🟢 MODERATE'
        ELSE '⚪ FAST'
    END AS performance_tier
FROM O2C_ENH_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY model_name, schema_name
ORDER BY avg_seconds DESC
LIMIT 20;

COMMENT ON VIEW O2C_ENH_SLOWEST_MODELS IS 
    'Top 20 slowest O2C Enhanced models by average execution time';

SELECT '✅ VIEW 4 CREATED: O2C_ENH_SLOWEST_MODELS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 5: SOURCE FRESHNESS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_SOURCE_FRESHNESS AS
-- Fact tables (use CREATED_DATE)
SELECT 
    'FACT_SALES_ORDERS' AS source_table,
    'Transactions' AS source_type,
    'CORP_TRAN' AS schema_name,
    COUNT(*) AS row_count,
    MAX(CREATED_DATE) AS last_load_timestamp,
    DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) AS hours_since_load,
    CASE 
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END AS freshness_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS

UNION ALL
SELECT 'FACT_INVOICES', 'Transactions', 'CORP_TRAN', COUNT(*), MAX(CREATED_DATE),
    DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
         ELSE '❌ Stale' END, CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_INVOICES

UNION ALL
SELECT 'FACT_PAYMENTS', 'Transactions', 'CORP_TRAN', COUNT(*), MAX(CREATED_DATE),
    DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(CREATED_DATE), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
         ELSE '❌ Stale' END, CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_PAYMENTS

UNION ALL
-- Dimension tables (use LOAD_TS)
SELECT 'DIM_CUSTOMER', 'Master', 'CORP_MASTER', COUNT(*), MAX(LOAD_TS),
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 48 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 72 THEN '⚠️ Warning'
         ELSE '❌ Stale' END, CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_CUSTOMER

UNION ALL
SELECT 'DIM_PAYMENT_TERMS', 'Master', 'CORP_MASTER', COUNT(*), MAX(LOAD_TS),
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 72 THEN '✅ Fresh'
         ELSE '⚠️ Warning' END, CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_PAYMENT_TERMS

UNION ALL
SELECT 'DIM_BANK_ACCOUNT', 'Master', 'CORP_MASTER', COUNT(*), MAX(LOAD_TS),
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) <= 72 THEN '✅ Fresh'
         ELSE '⚠️ Warning' END, CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_BANK_ACCOUNT;

COMMENT ON VIEW O2C_ENH_SOURCE_FRESHNESS IS 
    'O2C Enhanced source table freshness status';

SELECT '✅ VIEW 5 CREATED: O2C_ENH_SOURCE_FRESHNESS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 6: MODEL FRESHNESS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_MODEL_FRESHNESS AS
SELECT 
    'DIM_O2C_CUSTOMER' AS model_name,
    'DIMENSION' AS layer,
    COUNT(*) AS row_count,
    MAX(dbt_loaded_at) AS last_refresh,
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) AS minutes_since_refresh,
    CASE 
        WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
        WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
        ELSE '❌ Stale'
    END AS freshness_status
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER

UNION ALL
SELECT 'DM_O2C_RECONCILIATION', 'CORE', COUNT(*), MAX(dbt_updated_at),
    DATEDIFF('minute', MAX(dbt_updated_at), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(dbt_updated_at), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(dbt_updated_at), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
         ELSE '❌ Stale' END
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL
SELECT 'FACT_O2C_EVENTS', 'EVENTS', COUNT(*), MAX(dbt_loaded_at),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
         ELSE '❌ Stale' END
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS

UNION ALL
SELECT 'FACT_O2C_DAILY', 'PARTITIONED', COUNT(*), MAX(dbt_loaded_at),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
         ELSE '❌ Stale' END
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY

UNION ALL
SELECT 'AGG_O2C_BY_CUSTOMER', 'AGGREGATE', COUNT(*), MAX(dbt_loaded_at),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 24 THEN '✅ Fresh'
         WHEN DATEDIFF('hour', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) <= 48 THEN '⚠️ Warning'
         ELSE '❌ Stale' END
FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER;

COMMENT ON VIEW O2C_ENH_MODEL_FRESHNESS IS 
    'O2C Enhanced dbt model freshness status';

SELECT '✅ VIEW 6 CREATED: O2C_ENH_MODEL_FRESHNESS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 7: PERFORMANCE ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_PERFORMANCE AS
WITH baseline AS (
    SELECT 
        model_name,
        AVG(total_node_runtime) AS baseline_avg,
        STDDEV(total_node_runtime) AS baseline_stddev
    FROM O2C_ENH_MODEL_EXECUTIONS
    WHERE run_started_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'SUCCESS'
    GROUP BY model_name
),
recent AS (
    SELECT 
        model_name,
        AVG(total_node_runtime) AS recent_avg,
        MAX(total_node_runtime) AS recent_max,
        COUNT(*) AS recent_count
    FROM O2C_ENH_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -1, CURRENT_DATE())
      AND status = 'SUCCESS'
    GROUP BY model_name
)
SELECT 
    r.model_name,
    ROUND(b.baseline_avg, 2) AS baseline_seconds,
    ROUND(r.recent_avg, 2) AS recent_avg_seconds,
    ROUND(r.recent_max, 2) AS recent_max_seconds,
    ROUND(r.recent_avg - b.baseline_avg, 2) AS seconds_slower,
    ROUND((r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) * 100, 1) AS percent_slower,
    CASE 
        WHEN r.recent_avg > b.baseline_avg + (3 * b.baseline_stddev) THEN 'CRITICAL'
        WHEN r.recent_avg > b.baseline_avg + (2 * b.baseline_stddev) THEN 'HIGH'
        WHEN r.recent_avg > b.baseline_avg + (1 * b.baseline_stddev) THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    r.recent_count AS recent_run_count,
    CURRENT_TIMESTAMP() AS alert_time
FROM recent r
JOIN baseline b ON r.model_name = b.model_name
WHERE r.recent_avg > b.baseline_avg * 1.2  -- 20% slower threshold
ORDER BY percent_slower DESC;

COMMENT ON VIEW O2C_ENH_ALERT_PERFORMANCE IS 
    'O2C Enhanced performance degradation alerts';

SELECT '✅ VIEW 7 CREATED: O2C_ENH_ALERT_PERFORMANCE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 8: MODEL FAILURE ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_MODEL_FAILURES AS
WITH recent_failures AS (
    SELECT 
        model_name,
        schema_name,
        run_started_at AS failure_time,
        warehouse_name,
        total_node_runtime AS execution_seconds
    FROM O2C_ENH_MODEL_EXECUTIONS
    WHERE status = 'FAIL'
      AND run_started_at >= DATEADD(day, -7, CURRENT_DATE())
),
failure_counts AS (
    SELECT 
        model_name,
        COUNT(*) AS failures_last_7_days
    FROM recent_failures
    GROUP BY model_name
)
SELECT 
    rf.model_name,
    rf.schema_name,
    rf.failure_time,
    rf.warehouse_name,
    rf.execution_seconds,
    fc.failures_last_7_days,
    CASE 
        WHEN fc.failures_last_7_days >= 5 THEN 'CRITICAL'
        WHEN fc.failures_last_7_days >= 3 THEN 'HIGH'
        WHEN fc.failures_last_7_days >= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM recent_failures rf
JOIN failure_counts fc ON rf.model_name = fc.model_name
ORDER BY rf.failure_time DESC;

COMMENT ON VIEW O2C_ENH_ALERT_MODEL_FAILURES IS 
    'O2C Enhanced model failure alerts';

SELECT '✅ VIEW 8 CREATED: O2C_ENH_ALERT_MODEL_FAILURES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 9: STALE SOURCE ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_STALE_SOURCES AS
SELECT 
    source_table,
    source_type,
    row_count,
    last_load_timestamp,
    hours_since_load,
    CASE 
        WHEN hours_since_load > 72 THEN 'CRITICAL'
        WHEN hours_since_load > 48 THEN 'HIGH'
        WHEN hours_since_load > 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    'Source table ' || source_table || ' is ' || hours_since_load || ' hours old' AS alert_description,
    checked_at
FROM O2C_ENH_SOURCE_FRESHNESS
WHERE hours_since_load > 24
ORDER BY hours_since_load DESC;

COMMENT ON VIEW O2C_ENH_ALERT_STALE_SOURCES IS 
    'O2C Enhanced stale source alerts';

SELECT '✅ VIEW 9 CREATED: O2C_ENH_ALERT_STALE_SOURCES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 10: ALERT SUMMARY DASHBOARD
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_SUMMARY AS
SELECT 
    CURRENT_TIMESTAMP() AS snapshot_time,
    
    -- Performance Alerts
    (SELECT COUNT(*) FROM O2C_ENH_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') AS critical_performance_issues,
    (SELECT COUNT(*) FROM O2C_ENH_ALERT_PERFORMANCE WHERE severity = 'HIGH') AS high_performance_issues,
    
    -- Model Failures
    (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') AS critical_model_failures,
    (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'HIGH') AS high_model_failures,
    
    -- Source Staleness
    (SELECT COUNT(*) FROM O2C_ENH_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') AS critical_stale_sources,
    (SELECT COUNT(*) FROM O2C_ENH_ALERT_STALE_SOURCES WHERE severity = 'HIGH') AS high_stale_sources,
    
    -- Total Critical
    (
        (SELECT COUNT(*) FROM O2C_ENH_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') +
        (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') +
        (SELECT COUNT(*) FROM O2C_ENH_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL')
    ) AS total_critical_alerts,
    
    -- Health Score (0-100)
    GREATEST(0, 100 - (
        (SELECT COUNT(*) FROM O2C_ENH_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') * 15 +
        (SELECT COUNT(*) FROM O2C_ENH_ALERT_PERFORMANCE WHERE severity = 'HIGH') * 8 +
        (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') * 20 +
        (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'HIGH') * 10 +
        (SELECT COUNT(*) FROM O2C_ENH_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') * 10 +
        (SELECT COUNT(*) FROM O2C_ENH_ALERT_STALE_SOURCES WHERE severity = 'HIGH') * 5
    )) AS health_score,
    
    -- Health Status
    CASE 
        WHEN (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'CRITICAL') > 0 THEN '🔴 CRITICAL'
        WHEN (SELECT COUNT(*) FROM O2C_ENH_ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') > 0 THEN '🔴 CRITICAL'
        WHEN (SELECT COUNT(*) FROM O2C_ENH_ALERT_PERFORMANCE WHERE severity = 'CRITICAL') > 0 THEN '🟠 WARNING'
        WHEN (SELECT COUNT(DISTINCT model_name) FROM O2C_ENH_ALERT_MODEL_FAILURES WHERE severity = 'HIGH') > 0 THEN '🟡 ATTENTION'
        ELSE '🟢 HEALTHY'
    END AS health_status;

COMMENT ON VIEW O2C_ENH_ALERT_SUMMARY IS 
    'O2C Enhanced platform health summary dashboard';

SELECT '✅ VIEW 10 CREATED: O2C_ENH_ALERT_SUMMARY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 11: BUSINESS KPIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_BUSINESS_KPIS AS
SELECT 
    -- Volume Metrics
    COUNT(DISTINCT order_key) AS total_orders,
    COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) AS invoiced_orders,
    COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) AS paid_orders,
    
    -- Value Metrics
    SUM(order_amount) AS total_order_value,
    SUM(invoice_amount) AS total_invoice_value,
    SUM(payment_amount) AS total_payment_value,
    SUM(outstanding_amount) AS total_ar_outstanding,
    
    -- Performance Metrics
    ROUND(AVG(days_order_to_cash), 1) AS avg_dso,
    ROUND(MEDIAN(days_order_to_cash), 1) AS median_dso,
    MAX(days_order_to_cash) AS max_dso,
    
    -- Conversion Rates
    ROUND(COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) * 100.0 / 
          NULLIF(COUNT(DISTINCT order_key), 0), 1) AS billing_rate_pct,
    ROUND(COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END), 0), 1) AS collection_rate_pct,
    
    -- Payment Timing
    ROUND(SUM(CASE WHEN payment_timing = 'ON_TIME' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN payment_key != 'NOT_PAID' THEN 1 ELSE 0 END), 0), 1) AS on_time_payment_pct,
    
    -- Timestamp
    CURRENT_TIMESTAMP() AS snapshot_time
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION;

COMMENT ON VIEW O2C_ENH_BUSINESS_KPIS IS 
    'O2C Enhanced business KPIs and metrics';

SELECT '✅ VIEW 11 CREATED: O2C_ENH_BUSINESS_KPIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 12: ERROR LOG
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ERROR_LOG AS
SELECT 
    query_id,
    start_time AS error_time,
    user_name,
    warehouse_name,
    database_name,
    schema_name,
    query_type,
    error_code,
    error_message,
    LEFT(query_text, 1000) AS query_text_preview,
    CASE 
        WHEN error_message ILIKE '%syntax error%' THEN 'SYNTAX_ERROR'
        WHEN error_message ILIKE '%object does not exist%' THEN 'OBJECT_NOT_FOUND'
        WHEN error_message ILIKE '%access denied%' THEN 'ACCESS_DENIED'
        WHEN error_message ILIKE '%invalid identifier%' THEN 'INVALID_IDENTIFIER'
        WHEN error_message ILIKE '%timeout%' THEN 'TIMEOUT'
        WHEN error_message ILIKE '%resource limit%' THEN 'RESOURCE_LIMIT'
        ELSE 'OTHER'
    END AS error_category
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE execution_status = 'FAIL'
  AND database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED_%'
  AND start_time >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY error_time DESC;

COMMENT ON VIEW O2C_ENH_ERROR_LOG IS 
    'O2C Enhanced detailed error log';

SELECT '✅ VIEW 12 CREATED: O2C_ENH_ERROR_LOG' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 13: ERROR TREND
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ERROR_TREND AS
WITH daily_errors AS (
    SELECT 
        DATE_TRUNC('day', error_time) AS error_date,
        COUNT(*) AS error_count
    FROM O2C_ENH_ERROR_LOG
    WHERE error_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1
),
daily_queries AS (
    SELECT 
        DATE_TRUNC('day', start_time) AS query_date,
        COUNT(*) AS query_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND schema_name LIKE 'O2C_ENHANCED_%'
      AND start_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    q.query_date AS date,
    q.query_count AS total_queries,
    COALESCE(e.error_count, 0) AS error_count,
    q.query_count - COALESCE(e.error_count, 0) AS success_count,
    ROUND(COALESCE(e.error_count, 0) * 100.0 / NULLIF(q.query_count, 0), 2) AS error_rate_pct,
    ROUND((q.query_count - COALESCE(e.error_count, 0)) * 100.0 / NULLIF(q.query_count, 0), 2) AS success_rate_pct,
    AVG(COALESCE(e.error_count, 0)) OVER (ORDER BY q.query_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS error_rate_7day_avg
FROM daily_queries q
LEFT JOIN daily_errors e ON q.query_date = e.error_date
ORDER BY date DESC;

COMMENT ON VIEW O2C_ENH_ERROR_TREND IS 
    'O2C Enhanced error trend analysis';

SELECT '✅ VIEW 13 CREATED: O2C_ENH_ERROR_TREND' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 14: BUILD FAILURE DETAILS (Extended)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_BUILD_FAILURE_DETAILS AS
SELECT 
    query_id,
    start_time AS failure_time,
    end_time,
    user_name,
    warehouse_name,
    schema_name,
    query_type,
    error_code,
    error_message,
    LEFT(query_text, 2000) AS query_text_preview,
    
    -- Error classification
    CASE 
        WHEN error_message ILIKE '%syntax error%' THEN 'SYNTAX_ERROR'
        WHEN error_message ILIKE '%object does not exist%' THEN 'OBJECT_NOT_FOUND'
        WHEN error_message ILIKE '%access denied%' OR error_message ILIKE '%insufficient privileges%' THEN 'ACCESS_DENIED'
        WHEN error_message ILIKE '%invalid identifier%' THEN 'INVALID_IDENTIFIER'
        WHEN error_message ILIKE '%timeout%' OR error_message ILIKE '%exceeded%time%' THEN 'TIMEOUT'
        WHEN error_message ILIKE '%resource%' OR error_message ILIKE '%memory%' THEN 'RESOURCE_LIMIT'
        WHEN error_message ILIKE '%duplicate%' OR error_message ILIKE '%unique%constraint%' THEN 'CONSTRAINT_VIOLATION'
        WHEN error_message ILIKE '%null%' AND error_message ILIKE '%not null%' THEN 'NULL_CONSTRAINT'
        WHEN error_message ILIKE '%division by zero%' THEN 'DIVISION_BY_ZERO'
        WHEN error_message ILIKE '%conversion%' OR error_message ILIKE '%cast%' THEN 'TYPE_CONVERSION'
        ELSE 'OTHER'
    END AS error_category,
    
    -- Extract affected object
    COALESCE(
        REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(error_message, 'Object\\s+''([^'']+)''', 1, 1, 'ie', 1),
        'Unknown'
    ) AS affected_object,
    
    -- Timing
    ROUND(total_elapsed_time / 1000.0, 2) AS execution_seconds,
    DATEDIFF('minute', start_time, CURRENT_TIMESTAMP()) AS minutes_ago,
    
    -- Severity based on frequency and recency
    CASE 
        WHEN DATEDIFF('hour', start_time, CURRENT_TIMESTAMP()) <= 1 THEN 'CRITICAL'
        WHEN DATEDIFF('hour', start_time, CURRENT_TIMESTAMP()) <= 6 THEN 'HIGH'
        WHEN DATEDIFF('hour', start_time, CURRENT_TIMESTAMP()) <= 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS recency_severity,
    
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE execution_status = 'FAIL'
  AND database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND start_time >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY failure_time DESC;

COMMENT ON VIEW O2C_ENH_BUILD_FAILURE_DETAILS IS 
    'Detailed build failure analysis with error classification';

SELECT '✅ VIEW 14 CREATED: O2C_ENH_BUILD_FAILURE_DETAILS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 15: TEST SUMMARY BY TYPE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TEST_SUMMARY_BY_TYPE AS
SELECT 
    test_type,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS pass_rate_pct,
    ROUND(AVG(total_node_runtime), 2) AS avg_execution_sec,
    ROUND(MAX(total_node_runtime), 2) AS max_execution_sec,
    MAX(run_started_at) AS last_run,
    CASE 
        WHEN SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) = 100 THEN '✅ Excellent'
        WHEN SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) >= 95 THEN '✅ Good'
        WHEN SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) >= 90 THEN '⚠️ Fair'
        ELSE '❌ Poor'
    END AS health_status
FROM O2C_ENH_TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY test_type
ORDER BY pass_rate_pct ASC, total_executions DESC;

COMMENT ON VIEW O2C_ENH_TEST_SUMMARY_BY_TYPE IS 
    'Test execution summary grouped by test type';

SELECT '✅ VIEW 15 CREATED: O2C_ENH_TEST_SUMMARY_BY_TYPE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 16: TEST PASS RATE TREND
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TEST_PASS_RATE_TREND AS
SELECT 
    DATE(run_started_at) AS test_date,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS passed_tests,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_tests,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS pass_rate_pct,
    ROUND(AVG(total_node_runtime), 2) AS avg_test_duration_sec,
    -- 7-day moving average
    ROUND(AVG(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)) 
          OVER (ORDER BY DATE(run_started_at) ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS pass_rate_7day_avg
FROM O2C_ENH_TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY test_date
ORDER BY test_date DESC;

COMMENT ON VIEW O2C_ENH_TEST_PASS_RATE_TREND IS 
    'Daily test pass rate trend with 7-day moving average';

SELECT '✅ VIEW 16 CREATED: O2C_ENH_TEST_PASS_RATE_TREND' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 17: RECURRING TEST FAILURES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_RECURRING_TEST_FAILURES AS
WITH test_failures AS (
    SELECT 
        test_type,
        node_id,
        DATE(run_started_at) AS failure_date,
        COUNT(*) AS failures_on_date
    FROM O2C_ENH_TEST_EXECUTIONS
    WHERE status = 'FAIL'
      AND run_started_at >= DATEADD(day, -14, CURRENT_DATE())
    GROUP BY test_type, node_id, failure_date
)
SELECT 
    test_type,
    node_id AS test_identifier,
    COUNT(DISTINCT failure_date) AS days_with_failures,
    SUM(failures_on_date) AS total_failures,
    MIN(failure_date) AS first_failure,
    MAX(failure_date) AS last_failure,
    DATEDIFF('day', MIN(failure_date), MAX(failure_date)) + 1 AS failure_span_days,
    CASE 
        WHEN COUNT(DISTINCT failure_date) >= 7 THEN 'CRITICAL - Persistent'
        WHEN COUNT(DISTINCT failure_date) >= 4 THEN 'HIGH - Recurring'
        WHEN COUNT(DISTINCT failure_date) >= 2 THEN 'MEDIUM - Intermittent'
        ELSE 'LOW - Isolated'
    END AS severity,
    'Test failing for ' || COUNT(DISTINCT failure_date) || ' days, total ' || SUM(failures_on_date) || ' failures' AS alert_description
FROM test_failures
GROUP BY test_type, node_id
HAVING COUNT(DISTINCT failure_date) >= 2
ORDER BY total_failures DESC, days_with_failures DESC;

COMMENT ON VIEW O2C_ENH_RECURRING_TEST_FAILURES IS 
    'Tests with recurring failures over the past 14 days';

SELECT '✅ VIEW 17 CREATED: O2C_ENH_RECURRING_TEST_FAILURES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 18: EVENT TABLE ANALYTICS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_EVENT_ANALYTICS AS
SELECT 
    event_type,
    source_system,
    entity_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT entity_id) AS unique_entities,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(event_amount) AS total_amount,
    AVG(event_amount) AS avg_amount,
    MIN(event_timestamp) AS earliest_event,
    MAX(event_timestamp) AS latest_event,
    COUNT(DISTINCT DATE(event_timestamp)) AS active_days,
    -- Events per day
    ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT DATE(event_timestamp)), 0), 1) AS avg_events_per_day
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS
GROUP BY event_type, source_system, entity_type
ORDER BY event_count DESC;

COMMENT ON VIEW O2C_ENH_EVENT_ANALYTICS IS 
    'Event table analytics by type, source, and entity';

SELECT '✅ VIEW 18 CREATED: O2C_ENH_EVENT_ANALYTICS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 19: EVENT TIMELINE (Last 30 Days)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_EVENT_TIMELINE AS
SELECT 
    DATE(event_timestamp) AS event_date,
    event_type,
    COUNT(*) AS event_count,
    SUM(event_amount) AS total_amount,
    COUNT(DISTINCT customer_id) AS unique_customers,
    -- Running totals
    SUM(COUNT(*)) OVER (PARTITION BY event_type ORDER BY DATE(event_timestamp)) AS cumulative_events
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS
WHERE event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY event_date, event_type
ORDER BY event_date DESC, event_type;

COMMENT ON VIEW O2C_ENH_EVENT_TIMELINE IS 
    'Daily event timeline for the last 30 days';

SELECT '✅ VIEW 19 CREATED: O2C_ENH_EVENT_TIMELINE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 20: DATA QUALITY - ROW COUNT TRACKING
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ROW_COUNT_TRACKING AS
-- Source Layer
SELECT 
    'SOURCE' AS layer,
    'FACT_SALES_ORDERS' AS table_name,
    'Sales Orders' AS description,
    COUNT(*) AS row_count,
    MAX(CREATED_DATE) AS latest_record,
    CURRENT_TIMESTAMP() AS checked_at
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
-- Staging Layer
SELECT 'STAGING', 'STG_ENRICHED_ORDERS', 'Enriched Orders', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS

UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_INVOICES', 'Enriched Invoices', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES

UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_PAYMENTS', 'Enriched Payments', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS

UNION ALL
-- Dimension Layer
SELECT 'DIMENSION', 'DIM_O2C_CUSTOMER', 'O2C Customers', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER

UNION ALL
-- Core Layer
SELECT 'CORE', 'DM_O2C_RECONCILIATION', 'O2C Reconciliation', COUNT(*), MAX(dbt_updated_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL
-- Events Layer
SELECT 'EVENTS', 'FACT_O2C_EVENTS', 'O2C Events', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS

UNION ALL
-- Aggregates Layer
SELECT 'AGGREGATE', 'AGG_O2C_BY_CUSTOMER', 'Customer Aggregates', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER

ORDER BY 
    CASE layer
        WHEN 'SOURCE' THEN 1
        WHEN 'STAGING' THEN 2
        WHEN 'DIMENSION' THEN 3
        WHEN 'CORE' THEN 4
        WHEN 'EVENTS' THEN 5
        WHEN 'AGGREGATE' THEN 6
    END,
    table_name;

COMMENT ON VIEW O2C_ENH_ROW_COUNT_TRACKING IS 
    'Row count tracking across all O2C Enhanced layers';

SELECT '✅ VIEW 20 CREATED: O2C_ENH_ROW_COUNT_TRACKING' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 21: DATA QUALITY - SOURCE TO STAGING RECONCILIATION
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DATA_RECONCILIATION AS
WITH source_counts AS (
    SELECT 'ORDERS' AS entity, COUNT(*) AS source_rows FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
    UNION ALL
    SELECT 'INVOICES', COUNT(*) FROM EDW.CORP_TRAN.FACT_INVOICES
    UNION ALL
    SELECT 'PAYMENTS', COUNT(*) FROM EDW.CORP_TRAN.FACT_PAYMENTS
),
staging_counts AS (
    SELECT 'ORDERS' AS entity, COUNT(*) AS staging_rows FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS
    UNION ALL
    SELECT 'INVOICES', COUNT(*) FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES
    UNION ALL
    SELECT 'PAYMENTS', COUNT(*) FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS
)
SELECT 
    s.entity,
    s.source_rows,
    st.staging_rows,
    st.staging_rows - s.source_rows AS row_variance,
    ROUND((st.staging_rows - s.source_rows) * 100.0 / NULLIF(s.source_rows, 0), 2) AS variance_pct,
    CASE 
        WHEN st.staging_rows = s.source_rows THEN '✅ Matched'
        WHEN st.staging_rows > s.source_rows THEN '⚠️ Staging Higher'
        WHEN st.staging_rows < s.source_rows THEN '❌ Data Loss'
        ELSE '❓ Unknown'
    END AS validation_status,
    CURRENT_TIMESTAMP() AS validated_at
FROM source_counts s
JOIN staging_counts st ON s.entity = st.entity
ORDER BY entity;

COMMENT ON VIEW O2C_ENH_DATA_RECONCILIATION IS 
    'Source to staging row count reconciliation';

SELECT '✅ VIEW 21 CREATED: O2C_ENH_DATA_RECONCILIATION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 22: DATA QUALITY - NULL RATE ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_NULL_RATE_ANALYSIS AS
-- Core Reconciliation Table Null Analysis
SELECT 
    'DM_O2C_RECONCILIATION' AS table_name,
    'customer_name' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS null_rate_pct,
    CASE 
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) = 0 THEN '✅ Complete'
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 5 THEN '✅ Good'
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 20 THEN '⚠️ Moderate'
        ELSE '❌ High'
    END AS quality_status
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL
SELECT 'DM_O2C_RECONCILIATION', 'invoice_key', COUNT(*),
    SUM(CASE WHEN invoice_key = 'NOT_INVOICED' OR invoice_key IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN invoice_key = 'NOT_INVOICED' OR invoice_key IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE WHEN SUM(CASE WHEN invoice_key = 'NOT_INVOICED' OR invoice_key IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 30 THEN '✅ Expected'
         ELSE '⚠️ Review' END
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL
SELECT 'DM_O2C_RECONCILIATION', 'payment_key', COUNT(*),
    SUM(CASE WHEN payment_key = 'NOT_PAID' OR payment_key IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN payment_key = 'NOT_PAID' OR payment_key IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE WHEN SUM(CASE WHEN payment_key = 'NOT_PAID' OR payment_key IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 50 THEN '✅ Expected'
         ELSE '⚠️ Review' END
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL
SELECT 'DM_O2C_RECONCILIATION', 'days_order_to_cash', COUNT(*),
    SUM(CASE WHEN days_order_to_cash IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN days_order_to_cash IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE WHEN SUM(CASE WHEN days_order_to_cash IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 50 THEN '✅ Expected (unpaid)'
         ELSE '⚠️ Review' END
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

ORDER BY table_name, column_name;

COMMENT ON VIEW O2C_ENH_NULL_RATE_ANALYSIS IS 
    'Null rate analysis for critical columns';

SELECT '✅ VIEW 22 CREATED: O2C_ENH_NULL_RATE_ANALYSIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 23: DATA QUALITY - COMPLETENESS SCORECARD
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DATA_COMPLETENESS AS
WITH reconciliation_stats AS (
    SELECT 
        COUNT(*) AS total_orders,
        COUNT(CASE WHEN invoice_key IS NOT NULL AND invoice_key != 'NOT_INVOICED' THEN 1 END) AS invoiced_orders,
        COUNT(CASE WHEN payment_key IS NOT NULL AND payment_key != 'NOT_PAID' THEN 1 END) AS paid_orders,
        COUNT(CASE WHEN customer_name IS NOT NULL THEN 1 END) AS orders_with_customer,
        COUNT(CASE WHEN days_order_to_cash IS NOT NULL THEN 1 END) AS orders_with_dso
    FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
)
SELECT 
    'O2C Reconciliation' AS dataset,
    total_orders,
    
    -- Invoice completeness
    invoiced_orders,
    ROUND(invoiced_orders * 100.0 / NULLIF(total_orders, 0), 1) AS invoice_rate_pct,
    
    -- Payment completeness
    paid_orders,
    ROUND(paid_orders * 100.0 / NULLIF(total_orders, 0), 1) AS payment_rate_pct,
    
    -- Customer enrichment
    orders_with_customer,
    ROUND(orders_with_customer * 100.0 / NULLIF(total_orders, 0), 1) AS customer_enrichment_pct,
    
    -- DSO calculation completeness
    orders_with_dso,
    ROUND(orders_with_dso * 100.0 / NULLIF(total_orders, 0), 1) AS dso_calculation_pct,
    
    -- Overall completeness score
    ROUND((
        (orders_with_customer * 100.0 / NULLIF(total_orders, 0)) * 0.4 +  -- Customer is most important
        (invoiced_orders * 100.0 / NULLIF(total_orders, 0)) * 0.3 +
        (paid_orders * 100.0 / NULLIF(total_orders, 0)) * 0.2 +
        (orders_with_dso * 100.0 / NULLIF(total_orders, 0)) * 0.1
    ), 1) AS completeness_score,
    
    CASE 
        WHEN (orders_with_customer * 100.0 / NULLIF(total_orders, 0)) >= 95 THEN '✅ Excellent'
        WHEN (orders_with_customer * 100.0 / NULLIF(total_orders, 0)) >= 90 THEN '✅ Good'
        WHEN (orders_with_customer * 100.0 / NULLIF(total_orders, 0)) >= 80 THEN '⚠️ Fair'
        ELSE '❌ Poor'
    END AS quality_grade,
    
    CURRENT_TIMESTAMP() AS checked_at
FROM reconciliation_stats;

COMMENT ON VIEW O2C_ENH_DATA_COMPLETENESS IS 
    'Data completeness scorecard for O2C Enhanced';

SELECT '✅ VIEW 23 CREATED: O2C_ENH_DATA_COMPLETENESS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 24: EXECUTION TIMELINE (Gantt-style)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_EXECUTION_TIMELINE AS
SELECT 
    query_id,
    model_name,
    schema_name,
    run_started_at AS start_time,
    end_time,
    status,
    total_node_runtime AS duration_seconds,
    rows_affected,
    warehouse_name,
    
    -- For Gantt visualization
    DATEDIFF('second', 
        MIN(run_started_at) OVER (PARTITION BY DATE(run_started_at)), 
        run_started_at
    ) AS start_offset_seconds,
    
    -- Execution order within the day
    ROW_NUMBER() OVER (PARTITION BY DATE(run_started_at) ORDER BY run_started_at) AS execution_order,
    
    -- Time bucket for grouping
    DATE_TRUNC('hour', run_started_at) AS hour_bucket,
    
    user_name
FROM O2C_ENH_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY run_started_at DESC;

COMMENT ON VIEW O2C_ENH_EXECUTION_TIMELINE IS 
    'Model execution timeline for Gantt-style visualization';

SELECT '✅ VIEW 24 CREATED: O2C_ENH_EXECUTION_TIMELINE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 25: OPERATIONAL SUMMARY DASHBOARD
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_OPERATIONAL_SUMMARY AS
SELECT 
    CURRENT_TIMESTAMP() AS snapshot_time,
    
    -- Build Health (last 24 hours)
    (SELECT COUNT(*) FROM O2C_ENH_MODEL_EXECUTIONS WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS builds_24h,
    (SELECT COUNT(*) FROM O2C_ENH_MODEL_EXECUTIONS WHERE status = 'SUCCESS' AND run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS successful_builds_24h,
    (SELECT COUNT(*) FROM O2C_ENH_MODEL_EXECUTIONS WHERE status = 'FAIL' AND run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS failed_builds_24h,
    (SELECT ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) FROM O2C_ENH_MODEL_EXECUTIONS WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS build_success_rate_24h,
    
    -- Test Health (last 24 hours)
    (SELECT COUNT(*) FROM O2C_ENH_TEST_EXECUTIONS WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS tests_24h,
    (SELECT SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) FROM O2C_ENH_TEST_EXECUTIONS WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS passed_tests_24h,
    (SELECT SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) FROM O2C_ENH_TEST_EXECUTIONS WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS failed_tests_24h,
    (SELECT ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) FROM O2C_ENH_TEST_EXECUTIONS WHERE run_started_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS test_pass_rate_24h,
    
    -- Data Quality
    (SELECT completeness_score FROM O2C_ENH_DATA_COMPLETENESS) AS data_completeness_score,
    
    -- Freshness
    (SELECT COUNT(*) FROM O2C_ENH_SOURCE_FRESHNESS WHERE freshness_status LIKE '%Stale%') AS stale_sources,
    (SELECT COUNT(*) FROM O2C_ENH_MODEL_FRESHNESS WHERE freshness_status LIKE '%Stale%') AS stale_models,
    
    -- Events (last 24 hours)
    (SELECT COUNT(*) FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS WHERE event_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())) AS events_24h,
    
    -- Alerts
    (SELECT total_critical_alerts FROM O2C_ENH_ALERT_SUMMARY) AS critical_alerts,
    (SELECT health_score FROM O2C_ENH_ALERT_SUMMARY) AS platform_health_score,
    (SELECT health_status FROM O2C_ENH_ALERT_SUMMARY) AS platform_health_status;

COMMENT ON VIEW O2C_ENH_OPERATIONAL_SUMMARY IS 
    'Comprehensive operational summary dashboard';

SELECT '✅ VIEW 25 CREATED: O2C_ENH_OPERATIONAL_SUMMARY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

GRANT USAGE ON SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;

SELECT '✅ PERMISSIONS GRANTED' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ O2C ENHANCED MONITORING SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Show all created views
SHOW VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING;


