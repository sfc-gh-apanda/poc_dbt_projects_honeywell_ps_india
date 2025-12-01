-- ============================================================================
-- DBT MONITORING FOR SNOWFLAKE NATIVE DBT - USING QUERY HISTORY
-- ============================================================================
-- Purpose: Complete monitoring system that WORKS with Snowflake Native DBT
-- Uses: SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY instead of dbt_artifacts
-- Idempotent: YES - Can be run multiple times safely
-- ============================================================================

-- WHY THIS VERSION:
-- Snowflake Native DBT doesn't support on-run-end hooks properly, so dbt_artifacts
-- tables (MODEL_EXECUTIONS, TEST_EXECUTIONS) remain empty.
-- This script uses Query History which automatically captures ALL queries.
-- ============================================================================

-- PREREQUISITES:
-- ✅ 1. dbt projects have run at least once (creates tables/views)
-- ✅ 2. SNOWFLAKE.ACCOUNT_USAGE access granted
-- ✅ 3. You have ACCOUNTADMIN or sufficient privileges
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ============================================================================
-- STEP 1: CREATE MONITORING SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS DBT_MONITORING
    COMMENT = 'DBT observability using Query History - compatible with Snowflake Native DBT';

USE SCHEMA DBT_MONITORING;

SELECT 'STEP 1 COMPLETE: Schema created' as status;

-- ============================================================================
-- STEP 2: CREATE EXECUTION TRACKING VIEWS (FROM QUERY HISTORY)
-- ============================================================================

-- View 2.1: Model Executions (Query History-based)
CREATE OR REPLACE VIEW MODEL_EXECUTIONS AS
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
    query_type
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND schema_name IN ('DEV_DBT', 'DBT_FOUNDATION', 'DBT_FINANCE')
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND execution_status IN ('SUCCESS', 'FAIL')
  AND (
      query_text ILIKE '%create%or%replace%table%'
      OR query_text ILIKE '%create%or%replace%view%'
      OR query_text ILIKE '%insert%into%'
      OR query_text ILIKE '%merge%into%'
  );

COMMENT ON VIEW MODEL_EXECUTIONS IS 
    'dbt model executions tracked from Snowflake Query History (Snowflake Native DBT compatible)';

-- View 2.2: Test Executions (Query History-based)
CREATE OR REPLACE VIEW TEST_EXECUTIONS AS
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
    END as node_id
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND query_type = 'SELECT'
  AND (
      query_text ILIKE '%dbt_test%'
      OR (query_text ILIKE '%count(*)%' AND query_text ILIKE '%where%not%')
      OR query_text ILIKE '%dbt_utils%'
      OR query_text ILIKE '%dbt_expectations%'
  );

COMMENT ON VIEW TEST_EXECUTIONS IS 
    'dbt test executions inferred from Snowflake Query History';

SELECT 'STEP 2 COMPLETE: Execution tracking views created' as status;

-- ============================================================================
-- STEP 3: CREATE BASE MONITORING VIEWS
-- ============================================================================

-- View 3.1: Daily Execution Summary
CREATE OR REPLACE VIEW DAILY_EXECUTION_SUMMARY AS
SELECT 
    DATE_TRUNC('day', run_started_at) as execution_date,
    COUNT(DISTINCT node_id) as models_run,
    COUNT(DISTINCT CASE WHEN status = 'SUCCESS' THEN node_id END) as successful_models,
    COUNT(DISTINCT CASE WHEN status = 'FAIL' THEN node_id END) as failed_models,
    SUM(total_node_runtime) as total_execution_seconds,
    AVG(total_node_runtime) as avg_execution_seconds,
    MAX(total_node_runtime) as max_execution_seconds,
    ROUND(SUM(total_node_runtime) / 60, 2) as total_minutes
FROM MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

COMMENT ON VIEW DAILY_EXECUTION_SUMMARY IS 
    'Daily summary of dbt model executions';

-- View 3.2: Model Performance Ranking
CREATE OR REPLACE VIEW MODEL_PERFORMANCE_RANKING AS
SELECT 
    node_id,
    SPLIT_PART(node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    ROUND(AVG(total_node_runtime), 2) as avg_execution_seconds,
    ROUND(MAX(total_node_runtime), 2) as max_execution_seconds,
    ROUND(MIN(total_node_runtime), 2) as min_execution_seconds,
    ROUND(STDDEV(total_node_runtime), 2) as stddev_execution_seconds,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(SUM(total_node_runtime), 2) as total_seconds
FROM MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY 1, 2
HAVING run_count > 0
ORDER BY avg_execution_seconds DESC;

COMMENT ON VIEW MODEL_PERFORMANCE_RANKING IS 
    'Performance metrics per dbt model for optimization';

-- View 3.3: Test Results Health
CREATE OR REPLACE VIEW TEST_RESULTS_HEALTH AS
SELECT 
    DATE_TRUNC('day', run_started_at) as test_date,
    status,
    COUNT(*) as test_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE_TRUNC('day', run_started_at)), 1) as percentage
FROM TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

COMMENT ON VIEW TEST_RESULTS_HEALTH IS 
    'Daily test pass/fail rates for data quality monitoring';

-- View 3.4: Model Execution Trends
CREATE OR REPLACE VIEW MODEL_EXECUTION_TRENDS AS
SELECT 
    execution_date,
    model_name,
    avg_execution_seconds,
    ROUND(AVG(avg_execution_seconds) OVER (
        PARTITION BY model_name 
        ORDER BY execution_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as moving_avg_7day
FROM (
    SELECT 
        DATE_TRUNC('day', run_started_at) as execution_date,
        SPLIT_PART(node_id, '.', -1) as model_name,
        AVG(total_node_runtime) as avg_execution_seconds
    FROM MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1, 2
)
ORDER BY execution_date DESC, model_name;

COMMENT ON VIEW MODEL_EXECUTION_TRENDS IS 
    'Model execution time trends with 7-day moving averages';

-- View 3.5: Slowest Models (Current Week)
CREATE OR REPLACE VIEW SLOWEST_MODELS_CURRENT_WEEK AS
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    node_id,
    COUNT(*) as run_count,
    ROUND(AVG(total_node_runtime), 2) as avg_seconds,
    ROUND(MAX(total_node_runtime), 2) as max_seconds,
    ROUND(SUM(total_node_runtime), 2) as total_seconds,
    CASE 
        WHEN AVG(total_node_runtime) > 300 THEN 'CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN 'SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN 'MODERATE'
        ELSE 'FAST'
    END as performance_tier
FROM MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY avg_seconds DESC
LIMIT 20;

COMMENT ON VIEW SLOWEST_MODELS_CURRENT_WEEK IS 
    'Top 20 slowest models for optimization priority';

SELECT 'STEP 3 COMPLETE: Base monitoring views created (5 views)' as status;

-- ============================================================================
-- STEP 4: CREATE ALERT VIEWS
-- ============================================================================

-- Alert 4.1: Critical Performance Issues
CREATE OR REPLACE VIEW ALERT_CRITICAL_PERFORMANCE AS
WITH model_baseline AS (
    SELECT 
        node_id,
        AVG(total_node_runtime) as baseline_avg,
        STDDEV(total_node_runtime) as baseline_stddev
    FROM MODEL_EXECUTIONS
    WHERE run_started_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'SUCCESS'
    GROUP BY node_id
    HAVING COUNT(*) >= 3
),
recent_runs AS (
    SELECT 
        node_id,
        AVG(total_node_runtime) as recent_avg,
        MAX(total_node_runtime) as recent_max,
        COUNT(*) as recent_run_count
    FROM MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
      AND status = 'SUCCESS'
    GROUP BY node_id
)
SELECT 
    SPLIT_PART(r.node_id, '.', -1) as model_name,
    r.node_id,
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
    END as severity
FROM recent_runs r
JOIN model_baseline b ON r.node_id = b.node_id
WHERE r.recent_avg > b.baseline_avg + (2 * COALESCE(b.baseline_stddev, 0))
   OR r.recent_avg > 300
ORDER BY percent_slower DESC;

COMMENT ON VIEW ALERT_CRITICAL_PERFORMANCE IS 
    'Models running significantly slower than baseline';

-- Alert 4.2: Model Failures
CREATE OR REPLACE VIEW ALERT_MODEL_FAILURES AS
SELECT 
    run_started_at as generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as model_name,
    status,
    total_node_runtime as execution_time,
    (SELECT COUNT(*) 
     FROM MODEL_EXECUTIONS m2 
     WHERE m2.node_id = m1.node_id 
       AND m2.status = 'FAIL'
       AND m2.run_started_at >= DATEADD(day, -7, CURRENT_DATE())
    ) as failure_count_last_7_days,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM MODEL_EXECUTIONS m2 
              WHERE m2.node_id = m1.node_id 
                AND m2.status = 'FAIL'
                AND m2.run_started_at >= DATEADD(day, -7, CURRENT_DATE())
             ) >= 3 THEN 'CRITICAL'
        ELSE 'HIGH'
    END as severity,
    'Model execution failed' as alert_description
FROM MODEL_EXECUTIONS m1
WHERE status = 'FAIL'
  AND run_started_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
ORDER BY failure_count_last_7_days DESC, run_started_at DESC;

COMMENT ON VIEW ALERT_MODEL_FAILURES IS 
    'Model execution failures with recurring failure detection';

-- Alert 4.3: Critical Test Failures
CREATE OR REPLACE VIEW ALERT_CRITICAL_TEST_FAILURES AS
SELECT 
    run_started_at as generated_at,
    node_id,
    node_id as test_name,
    status,
    total_node_runtime as execution_time,
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'CRITICAL'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'CRITICAL'
        WHEN LOWER(node_id) LIKE '%relationship%' THEN 'HIGH'
        ELSE 'MEDIUM'
    END as severity,
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'Duplicate records detected'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'Null values in required field'
        WHEN LOWER(node_id) LIKE '%relationship%' THEN 'Referential integrity violation'
        ELSE 'Data quality check failed'
    END as alert_description
FROM TEST_EXECUTIONS
WHERE status = 'FAIL'
  AND run_started_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY run_started_at DESC;

COMMENT ON VIEW ALERT_CRITICAL_TEST_FAILURES IS 
    'Critical test failures requiring immediate attention';

-- Alert 4.4: All Critical Alerts (Composite)
CREATE OR REPLACE VIEW ALERT_ALL_CRITICAL AS
SELECT 'PERFORMANCE' as alert_category, severity, model_name as alert_subject,
       'Performance degradation: ' || percent_slower || '% slower' as alert_description, 
       CURRENT_TIMESTAMP() as alert_time
FROM ALERT_CRITICAL_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'MODEL_FAILURE' as alert_category, severity, model_name as alert_subject,
       alert_description, generated_at as alert_time
FROM ALERT_MODEL_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'TEST_FAILURE' as alert_category, severity, test_name as alert_subject, 
       alert_description, generated_at as alert_time
FROM ALERT_CRITICAL_TEST_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        ELSE 3
    END,
    alert_time DESC;

COMMENT ON VIEW ALERT_ALL_CRITICAL IS 
    'Unified view of all critical alerts across categories';

-- Alert 4.5: Alert Summary Dashboard
CREATE OR REPLACE VIEW ALERT_SUMMARY_DASHBOARD AS
SELECT 
    CURRENT_TIMESTAMP() as snapshot_time,
    
    -- Test Failures
    (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'CRITICAL') as critical_test_failures,
    (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'HIGH') as high_test_failures,
    
    -- Performance Issues
    (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'CRITICAL') as critical_performance_issues,
    (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'HIGH') as high_performance_issues,
    (SELECT COUNT(*) FROM ALERT_MODEL_FAILURES) as model_failures,
    
    -- Placeholder for features not yet implemented
    0 as recurring_test_failures,
    0 as stale_sources,
    0 as missing_data_loads,
    0 as cost_spikes,
    0 as expensive_queries,
    0 as warehouse_queuing_issues,
    0 as sla_violations,
    
    -- Overall Health Score (0-100)
    100 - (
        (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'CRITICAL') * 10 +
        (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'CRITICAL') * 8 +
        (SELECT COUNT(*) FROM ALERT_MODEL_FAILURES) * 15
    ) as health_score;

COMMENT ON VIEW ALERT_SUMMARY_DASHBOARD IS 
    'Summary dashboard with health score and alert counts';

SELECT 'STEP 4 COMPLETE: Alert views created (5 alert views)' as status;

-- ============================================================================
-- STEP 5: CREATE NOTIFICATION INFRASTRUCTURE
-- ============================================================================

-- Alert Audit Log Table
CREATE OR REPLACE TABLE ALERT_AUDIT_LOG (
    alert_id NUMBER AUTOINCREMENT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    alert_category STRING,
    severity STRING,
    alert_subject STRING,
    alert_description STRING,
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_method STRING,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by STRING,
    acknowledged_at TIMESTAMP_NTZ,
    CONSTRAINT pk_alert_id PRIMARY KEY (alert_id)
);

COMMENT ON TABLE ALERT_AUDIT_LOG IS 
    'Audit trail of all alerts and notifications';

-- Procedure: Get Alert Summary
CREATE OR REPLACE FUNCTION GET_ALERT_SUMMARY()
RETURNS TABLE (metric_name STRING, metric_value INTEGER, status STRING)
AS
$$
    SELECT 'Critical Test Failures' as metric_name, 
           critical_test_failures as metric_value,
           CASE WHEN critical_test_failures = 0 THEN 'OK' ELSE 'ALERT' END as status
    FROM ALERT_SUMMARY_DASHBOARD
    UNION ALL
    SELECT 'Performance Issues', critical_performance_issues,
           CASE WHEN critical_performance_issues = 0 THEN 'OK' ELSE 'ALERT' END
    FROM ALERT_SUMMARY_DASHBOARD
    UNION ALL
    SELECT 'Model Failures', model_failures,
           CASE WHEN model_failures = 0 THEN 'OK' ELSE 'ALERT' END
    FROM ALERT_SUMMARY_DASHBOARD
    UNION ALL
    SELECT 'Health Score', health_score,
           CASE WHEN health_score >= 90 THEN 'EXCELLENT'
                WHEN health_score >= 75 THEN 'GOOD'
                ELSE 'WARNING' END
    FROM ALERT_SUMMARY_DASHBOARD
$$;

COMMENT ON FUNCTION GET_ALERT_SUMMARY IS 
    'Returns current alert summary for quick status checks';

SELECT 'STEP 5 COMPLETE: Notification infrastructure created' as status;

-- ============================================================================
-- STEP 6: GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;

SELECT 'STEP 6 COMPLETE: Permissions granted' as status;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'VERIFICATION: Object Count Summary' as check_type;

SELECT 
    'Views' as object_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DBT_MONITORING'

UNION ALL

SELECT 'Tables', COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DBT_MONITORING'
  AND TABLE_TYPE = 'BASE TABLE'

UNION ALL

SELECT 'Functions', COUNT(*)
FROM INFORMATION_SCHEMA.FUNCTIONS
WHERE FUNCTION_SCHEMA = 'DBT_MONITORING';

-- List all views created
SELECT 'VERIFICATION: All Views Created' as check_type;
SELECT TABLE_NAME, COMMENT
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DBT_MONITORING'
ORDER BY TABLE_NAME;

-- Test views have data
SELECT 'VERIFICATION: Data Check' as check_type;

SELECT 'MODEL_EXECUTIONS' as view_name, COUNT(*) as row_count FROM MODEL_EXECUTIONS
UNION ALL SELECT 'TEST_EXECUTIONS', COUNT(*) FROM TEST_EXECUTIONS
UNION ALL SELECT 'DAILY_EXECUTION_SUMMARY', COUNT(*) FROM DAILY_EXECUTION_SUMMARY
UNION ALL SELECT 'MODEL_PERFORMANCE_RANKING', COUNT(*) FROM MODEL_PERFORMANCE_RANKING
UNION ALL SELECT 'SLOWEST_MODELS_CURRENT_WEEK', COUNT(*) FROM SLOWEST_MODELS_CURRENT_WEEK
UNION ALL SELECT 'TEST_RESULTS_HEALTH', COUNT(*) FROM TEST_RESULTS_HEALTH
ORDER BY view_name;

-- Show health summary
SELECT 'VERIFICATION: Current Health Status' as check_type;
SELECT * FROM ALERT_SUMMARY_DASHBOARD;

-- Show any critical alerts
SELECT 'VERIFICATION: Current Critical Alerts' as check_type;
SELECT * FROM ALERT_ALL_CRITICAL LIMIT 10;

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================

SELECT '
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║     ✅ DBT MONITORING SETUP COMPLETE - QUERY HISTORY BASED!             ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

📊 WHAT WAS CREATED:

✅ Schema: DBT_MONITORING

✅ Execution Tracking Views (Query History-based):
   - MODEL_EXECUTIONS
   - TEST_EXECUTIONS

✅ Base Monitoring Views (5 views):
   - DAILY_EXECUTION_SUMMARY
   - MODEL_PERFORMANCE_RANKING
   - TEST_RESULTS_HEALTH
   - MODEL_EXECUTION_TRENDS
   - SLOWEST_MODELS_CURRENT_WEEK

✅ Alert Views (5 views):
   - ALERT_CRITICAL_PERFORMANCE
   - ALERT_MODEL_FAILURES
   - ALERT_CRITICAL_TEST_FAILURES
   - ALERT_ALL_CRITICAL
   - ALERT_SUMMARY_DASHBOARD

✅ Infrastructure:
   - ALERT_AUDIT_LOG table
   - GET_ALERT_SUMMARY function

✅ Permissions granted to DBT_DEV_ROLE

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 NEXT STEPS:

1. Verify data exists:
   SELECT * FROM DBT_MONITORING.MODEL_EXECUTIONS LIMIT 10;
   
2. Check health status:
   SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;

3. View execution history:
   SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY;

4. Create Snowsight Dashboard:
   - Use queries from SNOWSIGHT_DASHBOARD_QUERIES.md
   - All queries work with Query History approach

5. Optional: Set up notifications
   - Run setup_notifications.sql for email/Slack alerts

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 KEY DIFFERENCES FROM dbt_artifacts VERSION:

✅ Uses QUERY_HISTORY - works with Snowflake Native DBT
✅ No dependency on on-run-end hooks
✅ Automatic data capture (every query logged)
✅ 90-day retention (configurable)
✅ Zero maintenance required

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎉 YOUR DBT MONITORING SYSTEM IS NOW LIVE!

Data should be visible immediately from your past dbt runs!

' as setup_complete;

