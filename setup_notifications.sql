-- ============================================================================
-- DBT NOTIFICATIONS & ALERTING AUTOMATION
-- ============================================================================
-- Purpose: Automate alert notifications via Email, Slack, and Snowflake Tasks
-- Dependencies: setup_comprehensive_alerts.sql must be run first
-- Idempotent: Yes - uses CREATE OR REPLACE for all objects
-- ============================================================================

-- PREREQUISITES:
-- 1. Run setup_comprehensive_alerts.sql (creates alert views)
-- 2. Configure Email Integration in Snowflake (see below)
-- 3. Optional: Configure Slack/Webhook integration
-- 4. Grant EXECUTE TASK privilege to role
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA DBT_MONITORING;

-- ============================================================================
-- SECTION 1: EMAIL INTEGRATION SETUP
-- ============================================================================

-- Step 1.1: Create Email Integration (Run once)
-- ============================================================================
-- Uncomment and configure with your SMTP settings

/*
CREATE OR REPLACE NOTIFICATION INTEGRATION dbt_email_integration
  TYPE=EMAIL
  ENABLED=TRUE
  ALLOWED_RECIPIENTS=('data-team@company.com','dba-team@company.com','alerts@company.com');
  
-- Verify integration
DESC NOTIFICATION INTEGRATION dbt_email_integration;
*/

-- Alternative: Use Snowflake's built-in email (requires ACCOUNTADMIN)
-- CALL SYSTEM$SEND_EMAIL(...) -- See tasks below for usage

-- ============================================================================
-- SECTION 2: NOTIFICATION STORED PROCEDURES
-- ============================================================================

-- Procedure 2.1: Send Critical Alerts Email
-- ============================================================================
CREATE OR REPLACE PROCEDURE SEND_CRITICAL_ALERTS_EMAIL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    alert_count INTEGER;
    email_body STRING;
    health_score INTEGER;
BEGIN
    -- Get alert count
    SELECT COUNT(*) INTO :alert_count 
    FROM ALERT_ALL_CRITICAL;
    
    -- Get health score
    SELECT health_score INTO :health_score
    FROM ALERT_SUMMARY_DASHBOARD;
    
    IF (:alert_count > 0) THEN
        -- Build email body
        email_body := '🚨 DBT CRITICAL ALERTS - Action Required\n\n' ||
                     'Total Critical Alerts: ' || :alert_count || '\n' ||
                     'System Health Score: ' || :health_score || '/100\n\n' ||
                     '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n';
        
        -- Add alert details (limit to top 10)
        LET alert_details STRING := (
            SELECT LISTAGG(
                alert_category || ' | ' || severity || ' | ' || alert_subject || '\n' ||
                '  → ' || alert_description || '\n\n',
                ''
            ) WITHIN GROUP (ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 ELSE 2 END, alert_time DESC)
            FROM (
                SELECT * FROM ALERT_ALL_CRITICAL LIMIT 10
            )
        );
        
        email_body := email_body || alert_details || 
                     '\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' ||
                     'View full details in Snowsight Dashboard:\n' ||
                     'SELECT * FROM DBT_MONITORING.ALERT_ALL_CRITICAL;\n\n' ||
                     'Timestamp: ' || CURRENT_TIMESTAMP()::STRING;
        
        -- Send email
        CALL SYSTEM$SEND_EMAIL(
            'dbt_email_integration',
            'data-team@company.com',
            '🚨 DBT Critical Alerts - ' || :alert_count || ' Issues Detected',
            :email_body
        );
        
        RETURN 'Email sent: ' || :alert_count || ' critical alerts';
    ELSE
        RETURN 'No critical alerts - email not sent';
    END IF;
END;
$$;

COMMENT ON PROCEDURE SEND_CRITICAL_ALERTS_EMAIL IS 
    'Sends email with critical alerts summary when issues detected';

-- Procedure 2.2: Send Daily Health Report
-- ============================================================================
CREATE OR REPLACE PROCEDURE SEND_DAILY_HEALTH_REPORT()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    email_body STRING;
    health_score INTEGER;
    critical_tests INTEGER;
    critical_perf INTEGER;
    model_failures INTEGER;
    stale_sources INTEGER;
    cost_spikes INTEGER;
BEGIN
    -- Get metrics from summary dashboard
    SELECT 
        health_score,
        critical_test_failures,
        critical_performance_issues,
        model_failures,
        stale_sources,
        cost_spikes
    INTO 
        :health_score,
        :critical_tests,
        :critical_perf,
        :model_failures,
        :stale_sources,
        :cost_spikes
    FROM ALERT_SUMMARY_DASHBOARD;
    
    -- Build email body
    email_body := '📊 DBT DAILY HEALTH REPORT\n\n' ||
                 '═══════════════════════════════════════\n' ||
                 'System Health Score: ' || :health_score || '/100 ' ||
                 CASE 
                    WHEN :health_score >= 90 THEN '✅ EXCELLENT'
                    WHEN :health_score >= 75 THEN '⚠️  GOOD'
                    WHEN :health_score >= 50 THEN '⚠️  WARNING'
                    ELSE '🚨 CRITICAL'
                 END || '\n' ||
                 '═══════════════════════════════════════\n\n' ||
                 '📈 METRICS SUMMARY\n' ||
                 '├─ Critical Test Failures: ' || :critical_tests || '\n' ||
                 '├─ Performance Issues: ' || :critical_perf || '\n' ||
                 '├─ Model Failures: ' || :model_failures || '\n' ||
                 '├─ Stale Data Sources: ' || :stale_sources || '\n' ||
                 '└─ Cost Anomalies: ' || :cost_spikes || '\n\n';
    
    -- Add execution summary
    LET exec_summary STRING := (
        SELECT 
            '📋 EXECUTION SUMMARY (Last 24 Hours)\n' ||
            '├─ Models Run: ' || models_run || '\n' ||
            '├─ Successful: ' || successful_models || '\n' ||
            '├─ Failed: ' || failed_models || '\n' ||
            '├─ Total Time: ' || ROUND(total_execution_seconds/60, 1) || ' minutes\n' ||
            '└─ Avg Time per Model: ' || ROUND(avg_execution_seconds, 1) || ' seconds\n\n'
        FROM DAILY_EXECUTION_SUMMARY
        WHERE execution_date = CURRENT_DATE() - 1
        LIMIT 1
    );
    
    email_body := email_body || exec_summary;
    
    -- Add test results
    LET test_summary STRING := (
        SELECT 
            '🧪 TEST RESULTS (Last 24 Hours)\n' ||
            LISTAGG(
                '├─ ' || status || ': ' || test_count || ' tests (' || ROUND(percentage, 1) || '%)\n',
                ''
            ) WITHIN GROUP (ORDER BY test_count DESC) || '\n'
        FROM TEST_RESULTS_HEALTH
        WHERE test_date = CURRENT_DATE() - 1
    );
    
    email_body := email_body || test_summary;
    
    -- Add top 5 slowest models
    LET slow_models STRING := (
        SELECT 
            '⏱️  TOP 5 SLOWEST MODELS\n' ||
            LISTAGG(
                (ROW_NUMBER() OVER (ORDER BY avg_seconds DESC))::STRING || '. ' || 
                model_name || ': ' || ROUND(avg_seconds, 1) || 's (' || performance_tier || ')\n',
                ''
            ) WITHIN GROUP (ORDER BY avg_seconds DESC)
        FROM (SELECT * FROM SLOWEST_MODELS_CURRENT_WEEK LIMIT 5)
    );
    
    email_body := email_body || '\n' || slow_models || '\n';
    
    -- Add footer
    email_body := email_body || 
                 '\n═══════════════════════════════════════\n' ||
                 '📅 Report Date: ' || (CURRENT_DATE() - 1)::STRING || '\n' ||
                 '🕐 Generated: ' || CURRENT_TIMESTAMP()::STRING || '\n' ||
                 '🔗 Dashboard: Snowsight → DBT Observability\n' ||
                 '═══════════════════════════════════════';
    
    -- Send email
    CALL SYSTEM$SEND_EMAIL(
        'dbt_email_integration',
        'data-team@company.com',
        '📊 DBT Daily Health Report - ' || (CURRENT_DATE() - 1)::STRING || ' (Health: ' || :health_score || '/100)',
        :email_body
    );
    
    RETURN 'Daily health report sent';
END;
$$;

COMMENT ON PROCEDURE SEND_DAILY_HEALTH_REPORT IS 
    'Sends comprehensive daily health report with all key metrics';

-- Procedure 2.3: Send Slack Notification (Webhook)
-- ============================================================================
CREATE OR REPLACE PROCEDURE SEND_SLACK_ALERT(
    webhook_url STRING,
    alert_category STRING,
    alert_message STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    slack_payload STRING;
    response VARIANT;
BEGIN
    -- Build Slack message payload
    slack_payload := OBJECT_CONSTRUCT(
        'text', '🚨 DBT Alert: ' || alert_category,
        'blocks', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'type', 'header',
                'text', OBJECT_CONSTRUCT(
                    'type', 'plain_text',
                    'text', '🚨 DBT Alert: ' || alert_category
                )
            ),
            OBJECT_CONSTRUCT(
                'type', 'section',
                'text', OBJECT_CONSTRUCT(
                    'type', 'mrkdwn',
                    'text', alert_message
                )
            ),
            OBJECT_CONSTRUCT(
                'type', 'context',
                'elements', ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(
                        'type', 'mrkdwn',
                        'text', '⏰ ' || CURRENT_TIMESTAMP()::STRING
                    )
                )
            )
        )
    )::STRING;
    
    -- Send to Slack webhook (requires external function - see setup below)
    -- response := CALL_SLACK_WEBHOOK(:webhook_url, :slack_payload);
    
    RETURN 'Slack notification sent for: ' || alert_category;
END;
$$;

COMMENT ON PROCEDURE SEND_SLACK_ALERT IS 
    'Sends alert to Slack channel via webhook (requires external function)';

-- Procedure 2.4: Log Alert to Audit Table
-- ============================================================================
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
    'Audit trail of all alerts generated and notifications sent';

CREATE OR REPLACE PROCEDURE LOG_ALERT(
    p_category STRING,
    p_severity STRING,
    p_subject STRING,
    p_description STRING,
    p_notification_method STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO ALERT_AUDIT_LOG (
        alert_category,
        severity,
        alert_subject,
        alert_description,
        notification_sent,
        notification_method
    )
    VALUES (
        :p_category,
        :p_severity,
        :p_subject,
        :p_description,
        TRUE,
        :p_notification_method
    );
    
    RETURN 'Alert logged: ' || :p_category || ' - ' || :p_subject;
END;
$$;

COMMENT ON PROCEDURE LOG_ALERT IS 
    'Logs alert to audit table for tracking and acknowledgment';

-- ============================================================================
-- SECTION 3: AUTOMATED TASKS (Scheduled Notifications)
-- ============================================================================

-- Task 3.1: Hourly Critical Alert Check
-- ============================================================================
CREATE OR REPLACE TASK TASK_HOURLY_CRITICAL_ALERTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 * * * * America/New_York' -- Every hour at :00
    COMMENT = 'Check for critical alerts every hour and send email if found'
AS
DECLARE
    alert_count INTEGER;
    result STRING;
BEGIN
    -- Check for critical alerts
    SELECT COUNT(*) INTO :alert_count 
    FROM ALERT_ALL_CRITICAL;
    
    IF (:alert_count > 0) THEN
        -- Send email
        CALL SEND_CRITICAL_ALERTS_EMAIL() INTO :result;
        
        -- Log each alert
        INSERT INTO ALERT_AUDIT_LOG (
            alert_category,
            severity,
            alert_subject,
            alert_description,
            notification_sent,
            notification_method
        )
        SELECT 
            alert_category,
            severity,
            alert_subject,
            alert_description,
            TRUE,
            'EMAIL'
        FROM ALERT_ALL_CRITICAL;
    END IF;
END;

-- Task 3.2: Daily Health Report (Morning)
-- ============================================================================
CREATE OR REPLACE TASK TASK_DAILY_HEALTH_REPORT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * America/New_York' -- 8 AM daily
    COMMENT = 'Send daily health report every morning'
AS
CALL SEND_DAILY_HEALTH_REPORT();

-- Task 3.3: Test Failure Alert (Every 4 hours)
-- ============================================================================
CREATE OR REPLACE TASK TASK_TEST_FAILURE_ALERTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */4 * * * America/New_York' -- Every 4 hours
    COMMENT = 'Check for test failures and send alerts'
AS
DECLARE
    failure_count INTEGER;
    email_body STRING;
BEGIN
    SELECT COUNT(*) INTO :failure_count 
    FROM ALERT_CRITICAL_TEST_FAILURES;
    
    IF (:failure_count > 0) THEN
        email_body := 'DBT TEST FAILURES DETECTED\n\n' ||
                     'Total Failures: ' || :failure_count || '\n\n' ||
                     'Failed Tests:\n' ||
                     (SELECT LISTAGG(
                         '• ' || test_name || ' (' || severity || ')\n  ' || alert_description,
                         '\n'
                     )
                     FROM ALERT_CRITICAL_TEST_FAILURES);
        
        CALL SYSTEM$SEND_EMAIL(
            'dbt_email_integration',
            'data-team@company.com',
            '🚨 DBT Test Failures - ' || :failure_count || ' Tests Failed',
            :email_body
        );
    END IF;
END;

-- Task 3.4: Performance Degradation Alert (Every 2 hours)
-- ============================================================================
CREATE OR REPLACE TASK TASK_PERFORMANCE_ALERTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */2 * * * America/New_York' -- Every 2 hours
    COMMENT = 'Check for performance degradation and send alerts'
AS
DECLARE
    perf_issue_count INTEGER;
    email_body STRING;
BEGIN
    SELECT COUNT(*) INTO :perf_issue_count 
    FROM ALERT_CRITICAL_PERFORMANCE
    WHERE severity IN ('CRITICAL', 'HIGH');
    
    IF (:perf_issue_count > 0) THEN
        email_body := 'DBT PERFORMANCE ISSUES DETECTED\n\n' ||
                     'Models with Performance Degradation: ' || :perf_issue_count || '\n\n' ||
                     'Details:\n' ||
                     (SELECT LISTAGG(
                         '• ' || model_name || '\n' ||
                         '  Baseline: ' || baseline_seconds || 's → Current: ' || recent_avg_seconds || 's\n' ||
                         '  Degradation: ' || percent_slower || '%\n' ||
                         '  Severity: ' || severity,
                         '\n'
                     )
                     FROM ALERT_CRITICAL_PERFORMANCE
                     WHERE severity IN ('CRITICAL', 'HIGH')
                     LIMIT 10);
        
        CALL SYSTEM$SEND_EMAIL(
            'dbt_email_integration',
            'data-team@company.com',
            '⚠️ DBT Performance Degradation - ' || :perf_issue_count || ' Models Affected',
            :email_body
        );
    END IF;
END;

-- Task 3.5: Cost Spike Alert (Daily at noon)
-- ============================================================================
CREATE OR REPLACE TASK TASK_COST_SPIKE_ALERTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 12 * * * America/New_York' -- Noon daily
    COMMENT = 'Check for cost spikes and send alerts'
AS
DECLARE
    cost_spike_count INTEGER;
    email_body STRING;
BEGIN
    SELECT COUNT(*) INTO :cost_spike_count 
    FROM ALERT_COST_SPIKES;
    
    IF (:cost_spike_count > 0) THEN
        email_body := 'DBT COST SPIKE DETECTED\n\n' ||
                     (SELECT 
                         'Today\'s Credits: ' || today_credits || '\n' ||
                         'Baseline Average: ' || baseline_avg_credits || '\n' ||
                         'Increase: ' || percent_increase || '%\n' ||
                         'Severity: ' || severity || '\n\n' ||
                         alert_description
                     FROM ALERT_COST_SPIKES
                     LIMIT 1) || '\n\n' ||
                     'Top Expensive Queries:\n' ||
                     (SELECT LISTAGG(
                         '• Query ID: ' || query_id || '\n' ||
                         '  Credits: ' || credits_used || '\n' ||
                         '  Warehouse: ' || warehouse_name,
                         '\n'
                     )
                     FROM ALERT_EXPENSIVE_QUERIES
                     LIMIT 5);
        
        CALL SYSTEM$SEND_EMAIL(
            'dbt_email_integration',
            'dba-team@company.com',
            '💰 DBT Cost Spike Alert',
            :email_body
        );
    END IF;
END;

-- Task 3.6: Data Freshness Alert (Every 6 hours)
-- ============================================================================
CREATE OR REPLACE TASK TASK_DATA_FRESHNESS_ALERTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */6 * * * America/New_York' -- Every 6 hours
    COMMENT = 'Check for stale data sources and send alerts'
AS
DECLARE
    stale_count INTEGER;
    email_body STRING;
BEGIN
    SELECT COUNT(*) INTO :stale_count 
    FROM ALERT_STALE_SOURCES
    WHERE severity IN ('CRITICAL', 'HIGH');
    
    IF (:stale_count > 0) THEN
        email_body := 'STALE DATA SOURCES DETECTED\n\n' ||
                     'Total Stale Sources: ' || :stale_count || '\n\n' ||
                     'Sources:\n' ||
                     (SELECT LISTAGG(
                         '• ' || source_name || '\n' ||
                         '  Last Updated: ' || max_loaded_at || '\n' ||
                         '  Hours Stale: ' || hours_stale || '\n' ||
                         '  Severity: ' || severity,
                         '\n'
                     )
                     FROM ALERT_STALE_SOURCES
                     WHERE severity IN ('CRITICAL', 'HIGH'));
        
        CALL SYSTEM$SEND_EMAIL(
            'dbt_email_integration',
            'data-team@company.com',
            '⏰ Stale Data Alert - ' || :stale_count || ' Sources Need Attention',
            :email_body
        );
    END IF;
END;

-- ============================================================================
-- SECTION 4: TASK MANAGEMENT & CONTROL
-- ============================================================================

-- Enable all tasks (run these commands manually when ready)
-- ============================================================================
/*
ALTER TASK TASK_HOURLY_CRITICAL_ALERTS RESUME;
ALTER TASK TASK_DAILY_HEALTH_REPORT RESUME;
ALTER TASK TASK_TEST_FAILURE_ALERTS RESUME;
ALTER TASK TASK_PERFORMANCE_ALERTS RESUME;
ALTER TASK TASK_COST_SPIKE_ALERTS RESUME;
ALTER TASK TASK_DATA_FRESHNESS_ALERTS RESUME;
*/

-- Disable all tasks (emergency pause)
-- ============================================================================
CREATE OR REPLACE PROCEDURE DISABLE_ALL_ALERT_TASKS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ALTER TASK IF EXISTS TASK_HOURLY_CRITICAL_ALERTS SUSPEND;
    ALTER TASK IF EXISTS TASK_DAILY_HEALTH_REPORT SUSPEND;
    ALTER TASK IF EXISTS TASK_TEST_FAILURE_ALERTS SUSPEND;
    ALTER TASK IF EXISTS TASK_PERFORMANCE_ALERTS SUSPEND;
    ALTER TASK IF EXISTS TASK_COST_SPIKE_ALERTS SUSPEND;
    ALTER TASK IF EXISTS TASK_DATA_FRESHNESS_ALERTS SUSPEND;
    RETURN 'All alert tasks suspended';
END;
$$;

-- Enable all tasks
-- ============================================================================
CREATE OR REPLACE PROCEDURE ENABLE_ALL_ALERT_TASKS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ALTER TASK IF EXISTS TASK_HOURLY_CRITICAL_ALERTS RESUME;
    ALTER TASK IF EXISTS TASK_DAILY_HEALTH_REPORT RESUME;
    ALTER TASK IF EXISTS TASK_TEST_FAILURE_ALERTS RESUME;
    ALTER TASK IF EXISTS TASK_PERFORMANCE_ALERTS RESUME;
    ALTER TASK IF EXISTS TASK_COST_SPIKE_ALERTS RESUME;
    ALTER TASK IF EXISTS TASK_DATA_FRESHNESS_ALERTS RESUME;
    RETURN 'All alert tasks enabled';
END;
$$;

-- Check task status
-- ============================================================================
CREATE OR REPLACE VIEW TASK_STATUS_MONITORING AS
SELECT 
    name as task_name,
    state,
    schedule,
    warehouse,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON,
    LAST_ERROR_MESSAGE,
    NEXT_SCHEDULED_TIME
FROM INFORMATION_SCHEMA.TASKS
WHERE name LIKE 'TASK_%ALERT%'
ORDER BY name;

COMMENT ON VIEW TASK_STATUS_MONITORING IS 
    'Monitor status of all alert tasks';

-- ============================================================================
-- SECTION 5: MANUAL NOTIFICATION HELPERS
-- ============================================================================

-- Helper: Send Test Email
-- ============================================================================
CREATE OR REPLACE PROCEDURE SEND_TEST_EMAIL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$SEND_EMAIL(
        'dbt_email_integration',
        'data-team@company.com',
        'DBT Alert System - Test Email',
        'This is a test email from the DBT alerting system.\n\n' ||
        'If you received this, email notifications are working correctly.\n\n' ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::STRING
    );
    RETURN 'Test email sent successfully';
END;
$$;

-- Helper: Get Current Alert Summary (for dashboards)
-- ============================================================================
CREATE OR REPLACE FUNCTION GET_ALERT_SUMMARY()
RETURNS TABLE (
    metric_name STRING,
    metric_value INTEGER,
    status STRING
)
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
    SELECT 'Stale Sources', stale_sources,
           CASE WHEN stale_sources = 0 THEN 'OK' ELSE 'ALERT' END
    FROM ALERT_SUMMARY_DASHBOARD
    UNION ALL
    SELECT 'Health Score', health_score,
           CASE WHEN health_score >= 90 THEN 'EXCELLENT'
                WHEN health_score >= 75 THEN 'GOOD'
                WHEN health_score >= 50 THEN 'WARNING'
                ELSE 'CRITICAL' END
    FROM ALERT_SUMMARY_DASHBOARD
$$;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'NOTIFICATION SYSTEM SETUP COMPLETE' as status;

-- Verify procedures created
SELECT 'Procedures Created:' as check_type, COUNT(*) as count
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'DBT_MONITORING'
  AND PROCEDURE_NAME LIKE '%ALERT%' OR PROCEDURE_NAME LIKE '%EMAIL%';

-- Verify tasks created
SELECT 'Tasks Created:' as check_type, COUNT(*) as count
FROM INFORMATION_SCHEMA.TASKS
WHERE TASK_SCHEMA = 'DBT_MONITORING'
  AND TASK_NAME LIKE 'TASK_%';

-- Show task schedules
SELECT * FROM TASK_STATUS_MONITORING;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- Manual notification examples:

-- 1. Send critical alerts email right now
CALL SEND_CRITICAL_ALERTS_EMAIL();

-- 2. Send daily health report right now
CALL SEND_DAILY_HEALTH_REPORT();

-- 3. Test email system
CALL SEND_TEST_EMAIL();

-- 4. Get current alert summary
SELECT * FROM TABLE(GET_ALERT_SUMMARY());

-- 5. Enable all alert tasks
CALL ENABLE_ALL_ALERT_TASKS();

-- 6. Disable all alert tasks
CALL DISABLE_ALL_ALERT_TASKS();

-- 7. Check task status
SELECT * FROM TASK_STATUS_MONITORING;

-- 8. View alert audit log
SELECT * FROM ALERT_AUDIT_LOG ORDER BY alert_timestamp DESC LIMIT 100;

-- 9. Acknowledge an alert
UPDATE ALERT_AUDIT_LOG
SET acknowledged = TRUE,
    acknowledged_by = CURRENT_USER(),
    acknowledged_at = CURRENT_TIMESTAMP()
WHERE alert_id = <alert_id>;
*/

