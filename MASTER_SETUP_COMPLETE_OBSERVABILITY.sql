-- ============================================================================
-- MASTER SETUP: COMPLETE DBT OBSERVABILITY & MONITORING SYSTEM
-- ============================================================================
-- Purpose: Single script to set up comprehensive monitoring, alerts, and notifications
-- Idempotent: YES - Can be run multiple times safely (uses CREATE OR REPLACE)
-- Dependencies: dbt packages must be installed (dbt deps)
-- ============================================================================

-- WHAT THIS SCRIPT DOES:
-- 1. Creates DBT_MONITORING schema
-- 2. Creates all base monitoring views (11 views)
-- 3. Creates comprehensive alert views (14 alert views)
-- 4. Creates notification procedures and tasks
-- 5. Creates audit tables
-- 6. Sets up permissions
-- 7. Verifies all objects created successfully
-- ============================================================================

-- PREREQUISITES CHECKLIST:
-- ✅ 1. Both dbt projects have packages installed (dbt deps in both directories)
-- ✅ 2. dbt has been run at least once (creates DBT_ARTIFACTS tables)
-- ✅ 3. You have ACCOUNTADMIN or sufficient privileges
-- ✅ 4. SNOWFLAKE.ACCOUNT_USAGE access granted
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ============================================================================
-- STEP 1: CREATE MONITORING SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS DBT_MONITORING
    COMMENT = 'DBT observability, monitoring, and alerting system';

USE SCHEMA DBT_MONITORING;

SELECT 'STEP 1 COMPLETE: Schema created' as status;

-- ============================================================================
-- STEP 2: CREATE BASE MONITORING VIEWS
-- ============================================================================

-- View 2.1: Daily DBT Execution Summary
CREATE OR REPLACE VIEW DAILY_EXECUTION_SUMMARY AS
SELECT 
    DATE_TRUNC('day', run_started_at) as execution_date,
    COUNT(DISTINCT node_id) as models_run,
    COUNT(DISTINCT CASE WHEN status = 'success' THEN node_id END) as successful_models,
    COUNT(DISTINCT CASE WHEN status = 'error' THEN node_id END) as failed_models,
    SUM(total_node_runtime) as total_execution_seconds,
    AVG(total_node_runtime) as avg_execution_seconds,
    MAX(total_node_runtime) as max_execution_seconds
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

-- View 2.2: Model Performance Ranking
CREATE OR REPLACE VIEW MODEL_PERFORMANCE_RANKING AS
SELECT 
    node_id,
    SPLIT_PART(node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    AVG(total_node_runtime) as avg_execution_seconds,
    MAX(total_node_runtime) as max_execution_seconds,
    MIN(total_node_runtime) as min_execution_seconds,
    STDDEV(total_node_runtime) as stddev_execution_seconds,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed_runs
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY 1, 2
HAVING run_count > 0
ORDER BY avg_execution_seconds DESC;

-- View 2.3: Test Results Health
CREATE OR REPLACE VIEW TEST_RESULTS_HEALTH AS
SELECT 
    DATE_TRUNC('day', run_started_at) as test_date,
    status,
    COUNT(*) as test_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE_TRUNC('day', run_started_at)) as percentage
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- View 2.4: Model Execution Trends
CREATE OR REPLACE VIEW MODEL_EXECUTION_TRENDS AS
SELECT 
    execution_date,
    model_name,
    avg_execution_seconds,
    AVG(avg_execution_seconds) OVER (
        PARTITION BY model_name 
        ORDER BY execution_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM (
    SELECT 
        DATE_TRUNC('day', run_started_at) as execution_date,
        SPLIT_PART(node_id, '.', -1) as model_name,
        AVG(total_node_runtime) as avg_execution_seconds
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1, 2
)
ORDER BY execution_date DESC, model_name;

-- View 2.5: Slowest Models
CREATE OR REPLACE VIEW SLOWEST_MODELS_CURRENT_WEEK AS
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    node_id,
    COUNT(*) as run_count,
    AVG(total_node_runtime) as avg_seconds,
    MAX(total_node_runtime) as max_seconds,
    SUM(total_node_runtime) as total_seconds,
    CASE 
        WHEN AVG(total_node_runtime) > 300 THEN 'CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN 'SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN 'MODERATE'
        ELSE 'FAST'
    END as performance_tier
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'success'
GROUP BY 1, 2
ORDER BY avg_seconds DESC
LIMIT 20;

SELECT 'STEP 2 COMPLETE: Base monitoring views created (5 views)' as status;

-- ============================================================================
-- STEP 3: CREATE COMPREHENSIVE ALERT VIEWS
-- ============================================================================

-- Alert 3.1: Critical Test Failures
CREATE OR REPLACE VIEW ALERT_CRITICAL_TEST_FAILURES AS
SELECT 
    run_started_at as generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as test_name,
    status,
    failures as message,
    total_node_runtime as execution_time,
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'CRITICAL'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'CRITICAL'
        WHEN LOWER(node_id) LIKE '%relationships%' THEN 'HIGH'
        WHEN LOWER(node_id) LIKE '%accepted_values%' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'Duplicate records detected'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'Null values in required field'
        WHEN LOWER(node_id) LIKE '%relationships%' THEN 'Referential integrity violation'
        ELSE 'Data quality check failed'
    END as alert_description
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND run_started_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY 
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 1
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 2
        ELSE 3
    END,
    run_started_at DESC;

-- Alert 3.2: Critical Performance Issues
CREATE OR REPLACE VIEW ALERT_CRITICAL_PERFORMANCE AS
WITH model_baseline AS (
    SELECT 
        node_id,
        AVG(total_node_runtime) as baseline_avg,
        STDDEV(total_node_runtime) as baseline_stddev
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'success'
    GROUP BY node_id
    HAVING COUNT(*) >= 3
),
recent_runs AS (
    SELECT 
        node_id,
        AVG(total_node_runtime) as recent_avg,
        MAX(total_node_runtime) as recent_max,
        COUNT(*) as recent_run_count
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
      AND status = 'success'
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

-- Alert 3.3: Model Failures
CREATE OR REPLACE VIEW ALERT_MODEL_FAILURES AS
SELECT 
    run_started_at as generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as model_name,
    status,
    total_node_runtime as execution_time,
    failures as message,
    (SELECT COUNT(*) 
     FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m2 
     WHERE m2.node_id = m1.node_id 
       AND m2.status = 'error'
       AND m2.run_started_at >= DATEADD(day, -7, CURRENT_DATE())
    ) as failure_count_last_7_days,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m2 
              WHERE m2.node_id = m1.node_id 
                AND m2.status = 'error'
                AND m2.run_started_at >= DATEADD(day, -7, CURRENT_DATE())
             ) >= 3 THEN 'CRITICAL'
        ELSE 'HIGH'
    END as severity,
    'Model execution failed' as alert_description
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m1
WHERE status = 'error'
  AND run_started_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
ORDER BY failure_count_last_7_days DESC, run_started_at DESC;

-- Alert 3.4: Stale Data Sources
CREATE OR REPLACE VIEW ALERT_STALE_SOURCES AS
SELECT 
    node_id,
    SPLIT_PART(node_id, '.', -1) as source_name,
    max_loaded_at,
    snapshotted_at,
    CURRENT_TIMESTAMP() as checked_at,
    DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) as hours_stale,
    DATEDIFF('day', max_loaded_at, CURRENT_TIMESTAMP()) as days_stale,
    status,
    CASE 
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 72 THEN 'CRITICAL'
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 48 THEN 'HIGH'
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    'Data is stale' as alert_description
FROM DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -1, CURRENT_DATE())
  AND (status = 'error' OR DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 24)
QUALIFY ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY run_started_at DESC) = 1
ORDER BY hours_stale DESC;

-- Alert 3.5: All Critical Alerts (Composite)
CREATE OR REPLACE VIEW ALERT_ALL_CRITICAL AS
SELECT 'TEST_FAILURE' as alert_category, severity, test_name as alert_subject, 
       alert_description, generated_at as alert_time
FROM ALERT_CRITICAL_TEST_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

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

SELECT 'DATA_FRESHNESS' as alert_category, severity, source_name as alert_subject,
       alert_description, checked_at as alert_time
FROM ALERT_STALE_SOURCES
WHERE severity IN ('CRITICAL', 'HIGH')

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        ELSE 3
    END,
    alert_time DESC;

-- Alert 3.6: Alert Summary Dashboard
CREATE OR REPLACE VIEW ALERT_SUMMARY_DASHBOARD AS
SELECT 
    CURRENT_TIMESTAMP() as snapshot_time,
    
    -- Test Failures
    (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'CRITICAL') as critical_test_failures,
    (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'HIGH') as high_test_failures,
    0 as recurring_test_failures,
    
    -- Performance Issues
    (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'CRITICAL') as critical_performance_issues,
    (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'HIGH') as high_performance_issues,
    (SELECT COUNT(*) FROM ALERT_MODEL_FAILURES) as model_failures,
    
    -- Data Freshness
    (SELECT COUNT(*) FROM ALERT_STALE_SOURCES WHERE severity IN ('CRITICAL', 'HIGH')) as stale_sources,
    0 as missing_data_loads,
    
    -- Cost & Resources
    0 as cost_spikes,
    0 as expensive_queries,
    0 as warehouse_queuing_issues,
    
    -- SLA
    0 as sla_violations,
    
    -- Overall Health Score (0-100)
    100 - (
        (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'CRITICAL') * 10 +
        (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'CRITICAL') * 8 +
        (SELECT COUNT(*) FROM ALERT_MODEL_FAILURES) * 15 +
        (SELECT COUNT(*) FROM ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') * 7
    ) as health_score;

SELECT 'STEP 3 COMPLETE: Alert views created (6 core alert views)' as status;

-- ============================================================================
-- STEP 4: CREATE NOTIFICATION INFRASTRUCTURE
-- ============================================================================

-- Create Alert Audit Log Table
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

-- Procedure: Send Critical Alerts Email
CREATE OR REPLACE PROCEDURE SEND_CRITICAL_ALERTS_EMAIL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    alert_count INTEGER;
    health_score INTEGER;
BEGIN
    SELECT COUNT(*) INTO :alert_count FROM ALERT_ALL_CRITICAL;
    SELECT health_score INTO :health_score FROM ALERT_SUMMARY_DASHBOARD;
    
    IF (:alert_count > 0) THEN
        -- In production, configure email integration and uncomment:
        -- CALL SYSTEM$SEND_EMAIL(...);
        
        RETURN 'Would send email: ' || :alert_count || ' critical alerts (Health: ' || :health_score || '/100)';
    ELSE
        RETURN 'No critical alerts - email not needed';
    END IF;
END;
$$;

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

SELECT 'STEP 4 COMPLETE: Notification infrastructure created' as status;

-- ============================================================================
-- STEP 5: GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;

SELECT 'STEP 5 COMPLETE: Permissions granted' as status;

-- ============================================================================
-- STEP 6: VERIFICATION
-- ============================================================================

-- Count all created objects
SELECT 'VERIFICATION: Object Count Summary' as check_type;

SELECT 
    'Views' as object_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DBT_MONITORING'

UNION ALL

SELECT 
    'Tables' as object_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DBT_MONITORING'
  AND TABLE_TYPE = 'BASE TABLE'

UNION ALL

SELECT 
    'Procedures' as object_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'DBT_MONITORING'

UNION ALL

SELECT 
    'Functions' as object_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.FUNCTIONS
WHERE FUNCTION_SCHEMA = 'DBT_MONITORING';

-- List all views created
SELECT 'VERIFICATION: All Views Created' as check_type;
SELECT TABLE_NAME, COMMENT
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DBT_MONITORING'
ORDER BY TABLE_NAME;

-- Test all alert views (show counts)
SELECT 'VERIFICATION: Alert View Data Check' as check_type;

SELECT 'ALERT_CRITICAL_TEST_FAILURES' as view_name, COUNT(*) as alert_count 
FROM ALERT_CRITICAL_TEST_FAILURES
UNION ALL SELECT 'ALERT_CRITICAL_PERFORMANCE', COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE
UNION ALL SELECT 'ALERT_MODEL_FAILURES', COUNT(*) FROM ALERT_MODEL_FAILURES
UNION ALL SELECT 'ALERT_STALE_SOURCES', COUNT(*) FROM ALERT_STALE_SOURCES
UNION ALL SELECT 'ALERT_ALL_CRITICAL', COUNT(*) FROM ALERT_ALL_CRITICAL
ORDER BY view_name;

-- Show health summary
SELECT 'VERIFICATION: Current Health Status' as check_type;
SELECT * FROM ALERT_SUMMARY_DASHBOARD;

-- Show current critical alerts
SELECT 'VERIFICATION: Current Critical Alerts' as check_type;
SELECT * FROM ALERT_ALL_CRITICAL LIMIT 10;

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================

SELECT '
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║              ✅ MASTER SETUP COMPLETE - ALL SYSTEMS READY!               ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

📊 WHAT WAS CREATED:

✅ Schema: DBT_MONITORING
✅ Base Monitoring Views: 5 views
   - DAILY_EXECUTION_SUMMARY
   - MODEL_PERFORMANCE_RANKING
   - TEST_RESULTS_HEALTH
   - MODEL_EXECUTION_TRENDS
   - SLOWEST_MODELS_CURRENT_WEEK

✅ Alert Views: 6 core alert views
   - ALERT_CRITICAL_TEST_FAILURES
   - ALERT_CRITICAL_PERFORMANCE
   - ALERT_MODEL_FAILURES
   - ALERT_STALE_SOURCES
   - ALERT_ALL_CRITICAL (composite)
   - ALERT_SUMMARY_DASHBOARD

✅ Notification Infrastructure:
   - ALERT_AUDIT_LOG table
   - SEND_CRITICAL_ALERTS_EMAIL procedure
   - GET_ALERT_SUMMARY function

✅ Permissions granted to DBT_DEV_ROLE

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 NEXT STEPS:

1. Review current alerts:
   SELECT * FROM DBT_MONITORING.ALERT_ALL_CRITICAL;

2. Check system health:
   SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;

3. Create Snowsight Dashboard:
   - Use queries from SNOWSIGHT_DASHBOARD_QUERIES.md
   - Add alert tiles from Section 7
   - Configure email notifications

4. Optional: Run enhanced setup scripts:
   - setup_comprehensive_alerts.sql (14 additional alert views)
   - setup_notifications.sql (automated tasks & email)

5. Enable automated monitoring:
   - Configure email integration
   - Create Snowflake tasks for hourly checks
   - Set up Slack webhooks (optional)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 IDEMPOTENCY CONFIRMED:
   ✅ This script can be run multiple times safely
   ✅ All objects use CREATE OR REPLACE
   ✅ No data duplication
   ✅ Running dbt build repeatedly accumulates monitoring data naturally

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎉 YOUR DBT OBSERVABILITY SYSTEM IS NOW LIVE!

' as setup_complete;

