-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - NATIVE SNOWFLAKE ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Comprehensive native Snowflake alerts for O2C Enhanced observability
-- 
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │                           ALERT SUMMARY MATRIX                              │
-- ├────────────────────────────┬─────────────────────────────────┬─────────────┤
-- │ Alert                      │ Trigger Condition               │ Schedule    │
-- ├────────────────────────────┼─────────────────────────────────┼─────────────┤
-- │ ALERT_COST_SPIKE           │ Daily cost >50% above 7-day avg │ 8am & 6pm   │
-- │ ALERT_HIGH_COST_MODEL      │ Single model >$5/week           │ Daily 9am   │
-- │ ALERT_MONTHLY_BUDGET       │ Monthly cost >$100              │ Daily 9am   │
-- │ ALERT_QUEUE_TIME           │ Avg queue >10 seconds           │ Every 30min │
-- │ ALERT_LONG_RUNNING_QUERY   │ Query >5 minutes                │ Every 15min │
-- │ ALERT_MODEL_PERFORMANCE    │ >20% slower than baseline       │ Every 4hr   │
-- │ ALERT_SLOW_MODEL           │ Avg execution >5 minutes        │ Daily 10am  │
-- │ ALERT_INCREMENTAL_INEFFIC  │ <1000 rows/second               │ Weekly Mon  │
-- │ ALERT_SCHEMA_DRIFT         │ DDL changes detected            │ Every 30min │
-- │ ALERT_DBT_COVERAGE         │ Models without tests            │ Daily 11am  │
-- │ ALERT_DATA_INTEGRITY       │ PK/FK/Duplicate issues          │ Daily 8am   │
-- └────────────────────────────┴─────────────────────────────────┴─────────────┘
-- 
-- Prerequisites:
--   - O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql executed
--   - O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql executed
--   - O2C_ENHANCED_MONITORING_SETUP.sql executed
--   - Notification integrations configured
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_AUDIT;

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '🚀 STARTING: Native Snowflake Alerts Setup' AS status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: CREATE ALERT HISTORY TABLE (if not exists)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS O2C_AUDIT.O2C_ALERT_HISTORY (
    alert_id            VARCHAR(50) DEFAULT UUID_STRING() PRIMARY KEY,
    alert_name          VARCHAR(200) NOT NULL,
    alert_type          VARCHAR(50) NOT NULL,
    severity            VARCHAR(20) NOT NULL,
    condition_value     VARIANT,
    threshold_value     VARIANT,
    alert_message       VARCHAR(4000),
    affected_objects    ARRAY,
    triggered_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    acknowledged_at     TIMESTAMP_NTZ,
    acknowledged_by     VARCHAR(100),
    resolved_at         TIMESTAMP_NTZ,
    notification_sent   BOOLEAN DEFAULT FALSE,
    notification_channel VARCHAR(50),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Alert history for O2C Enhanced - consumable by Monte Carlo';

SELECT '✅ STEP 1: Alert history table ready' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: CREATE NOTIFICATION INTEGRATIONS (Placeholders - Update with actual values)
-- ═══════════════════════════════════════════════════════════════════════════════

-- NOTE: These are placeholder configurations. Update with actual webhook URLs.

-- Email Integration (requires proper email addresses)
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS O2C_EMAIL_NOTIFICATION
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = (
        'data-ops@company.com',
        'dbt-alerts@company.com'
    )
    COMMENT = 'O2C Enhanced email notifications';

-- Slack Integration (update with actual webhook)
-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS O2C_SLACK_NOTIFICATION
--     TYPE = WEBHOOK
--     ENABLED = TRUE
--     WEBHOOK_URL = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
--     COMMENT = 'O2C Enhanced Slack notifications';

SELECT '✅ STEP 2: Notification integrations configured (update webhooks as needed)' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: CREATE ALERT LOGGING PROCEDURE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE O2C_AUDIT.LOG_O2C_ALERT(
    p_alert_name VARCHAR,
    p_alert_type VARCHAR,
    p_severity VARCHAR,
    p_message VARCHAR,
    p_condition_value VARIANT,
    p_threshold_value VARIANT,
    p_affected_objects ARRAY
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO O2C_AUDIT.O2C_ALERT_HISTORY (
        alert_id,
        alert_name,
        alert_type,
        severity,
        condition_value,
        threshold_value,
        alert_message,
        affected_objects,
        triggered_at
    )
    VALUES (
        UUID_STRING(),
        :p_alert_name,
        :p_alert_type,
        :p_severity,
        :p_condition_value,
        :p_threshold_value,
        :p_message,
        :p_affected_objects,
        CURRENT_TIMESTAMP()
    );
    
    RETURN 'Alert logged: ' || :p_alert_name;
END;
$$;

COMMENT ON PROCEDURE O2C_AUDIT.LOG_O2C_ALERT(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARIANT, VARIANT, ARRAY) IS 
    'Logs alert to O2C_ALERT_HISTORY table';

SELECT '✅ STEP 3: Alert logging procedure created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 1: COST ALERTS
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 1: Creating Cost Alerts' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 1: DAILY COST SPIKE (>50% above 7-day average)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_COST_SPIKE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8,18 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST
        WHERE severity IN ('CRITICAL', 'HIGH')
          AND usage_date >= DATEADD('day', -1, CURRENT_DATE())
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Daily Cost Spike Detected',
        'COST',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST 
         WHERE usage_date >= DATEADD('day', -1, CURRENT_DATE())),
        (SELECT LISTAGG(warehouse_name || ': $' || ROUND(estimated_cost_usd, 2) || 
                ' (' || ROUND(COALESCE(variance_from_avg_pct, 0), 0) || '% variance)', ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST 
         WHERE severity IN ('CRITICAL', 'HIGH')
           AND usage_date >= DATEADD('day', -1, CURRENT_DATE())),
        (SELECT OBJECT_CONSTRUCT('max_variance_pct', MAX(variance_from_avg_pct))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST 
         WHERE usage_date >= DATEADD('day', -1, CURRENT_DATE())),
        OBJECT_CONSTRUCT('threshold_variance_pct', 50),
        (SELECT ARRAY_AGG(DISTINCT warehouse_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_COST 
         WHERE severity IN ('CRITICAL', 'HIGH')
           AND usage_date >= DATEADD('day', -1, CURRENT_DATE()))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_COST_SPIKE IS 
    'Triggers when daily cost exceeds 50% above 7-day average';

SELECT '✅ ALERT 1 CREATED: ALERT_COST_SPIKE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 2: HIGH COST MODEL (Single model >$5/week)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_HIGH_COST_MODEL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 9 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
        WHERE estimated_cost_usd > 5
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'High Cost Model Detected',
        'COST',
        CASE 
            WHEN (SELECT MAX(estimated_cost_usd) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL) > 20 THEN 'CRITICAL'
            WHEN (SELECT MAX(estimated_cost_usd) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL) > 10 THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        (SELECT LISTAGG(model_name || ': $' || ROUND(estimated_cost_usd, 2), ', ') WITHIN GROUP (ORDER BY estimated_cost_usd DESC)
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
         WHERE estimated_cost_usd > 5),
        (SELECT OBJECT_CONSTRUCT('max_cost_usd', MAX(estimated_cost_usd), 'models_count', COUNT(*))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
         WHERE estimated_cost_usd > 5),
        OBJECT_CONSTRUCT('threshold_cost_usd', 5),
        (SELECT ARRAY_AGG(model_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL 
         WHERE estimated_cost_usd > 5)
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_HIGH_COST_MODEL IS 
    'Triggers when a single model costs more than $5/week';

SELECT '✅ ALERT 2 CREATED: ALERT_HIGH_COST_MODEL' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 3: MONTHLY BUDGET EXCEEDED (>$100)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_MONTHLY_BUDGET
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 9 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY
        WHERE month = DATE_TRUNC('month', CURRENT_DATE())
          AND total_cost_usd > 100
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Monthly Cost Budget Exceeded',
        'COST',
        CASE 
            WHEN (SELECT total_cost_usd FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY 
                  WHERE month = DATE_TRUNC('month', CURRENT_DATE())) > 200 THEN 'CRITICAL'
            WHEN (SELECT total_cost_usd FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY 
                  WHERE month = DATE_TRUNC('month', CURRENT_DATE())) > 150 THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        (SELECT 'Monthly cost: $' || ROUND(total_cost_usd, 2) || 
                ' | Credits: ' || ROUND(total_credits, 2) ||
                ' | MoM: ' || COALESCE(mom_cost_change_pct::VARCHAR, 'N/A') || '%'
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY
         WHERE month = DATE_TRUNC('month', CURRENT_DATE())),
        (SELECT OBJECT_CONSTRUCT('total_cost', total_cost_usd, 'total_credits', total_credits)
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_COST_MONTHLY
         WHERE month = DATE_TRUNC('month', CURRENT_DATE())),
        OBJECT_CONSTRUCT('monthly_budget_usd', 100),
        ARRAY_CONSTRUCT('O2C_ENHANCED')
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_MONTHLY_BUDGET IS 
    'Triggers when monthly cost exceeds $100 budget';

SELECT '✅ ALERT 3 CREATED: ALERT_MONTHLY_BUDGET' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 2: QUERY PERFORMANCE ALERTS
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 2: Creating Query Performance Alerts' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 4: QUEUE TIME (>10 seconds average)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_QUEUE_TIME
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON */30 * * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_QUEUE
        WHERE severity IN ('CRITICAL', 'HIGH')
          AND hour_bucket >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Warehouse Queue Time High',
        'QUERY',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_QUEUE
         WHERE hour_bucket >= DATEADD('hour', -2, CURRENT_TIMESTAMP())),
        (SELECT LISTAGG(warehouse_name || ': ' || ROUND(avg_queue_seconds, 1) || 's avg queue', ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_QUEUE
         WHERE severity IN ('CRITICAL', 'HIGH')
           AND hour_bucket >= DATEADD('hour', -2, CURRENT_TIMESTAMP())),
        (SELECT OBJECT_CONSTRUCT('max_avg_queue', MAX(avg_queue_seconds), 'max_queue', MAX(max_queue_seconds))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_QUEUE
         WHERE hour_bucket >= DATEADD('hour', -2, CURRENT_TIMESTAMP())),
        OBJECT_CONSTRUCT('threshold_seconds', 10),
        (SELECT ARRAY_AGG(DISTINCT warehouse_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_QUEUE
         WHERE severity IN ('CRITICAL', 'HIGH')
           AND hour_bucket >= DATEADD('hour', -2, CURRENT_TIMESTAMP()))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_QUEUE_TIME IS 
    'Triggers when average queue time exceeds 10 seconds';

SELECT '✅ ALERT 4 CREATED: ALERT_QUEUE_TIME' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 5: LONG RUNNING QUERY (>5 minutes)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_LONG_RUNNING_QUERY
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON */15 * * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_LONG_QUERY
        WHERE start_time >= DATEADD('minute', -20, CURRENT_TIMESTAMP())
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Long Running Query Detected',
        'QUERY',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_LONG_QUERY
         WHERE start_time >= DATEADD('minute', -20, CURRENT_TIMESTAMP())),
        (SELECT LISTAGG(ROUND(elapsed_seconds / 60, 1) || ' min on ' || warehouse_name, ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_LONG_QUERY
         WHERE start_time >= DATEADD('minute', -20, CURRENT_TIMESTAMP())
         LIMIT 3),
        (SELECT OBJECT_CONSTRUCT('max_duration_seconds', MAX(elapsed_seconds), 'query_count', COUNT(*))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_LONG_QUERY
         WHERE start_time >= DATEADD('minute', -20, CURRENT_TIMESTAMP())),
        OBJECT_CONSTRUCT('threshold_seconds', 300),
        (SELECT ARRAY_AGG(query_id) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_LONG_QUERY
         WHERE start_time >= DATEADD('minute', -20, CURRENT_TIMESTAMP()))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_LONG_RUNNING_QUERY IS 
    'Triggers when queries run longer than 5 minutes';

SELECT '✅ ALERT 5 CREATED: ALERT_LONG_RUNNING_QUERY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 3: MODEL PERFORMANCE ALERTS
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 3: Creating Model Performance Alerts' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 6: MODEL PERFORMANCE DEGRADATION (>20% slower)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_MODEL_PERFORMANCE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */4 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE
        WHERE severity IN ('CRITICAL', 'HIGH')
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Model Performance Degradation',
        'PERFORMANCE',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE 
         WHERE severity IN ('CRITICAL', 'HIGH')),
        (SELECT LISTAGG(model_name || ': ' || ROUND(percent_slower, 0) || '% slower', ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE
         WHERE severity IN ('CRITICAL', 'HIGH')),
        (SELECT OBJECT_CONSTRUCT('max_slowdown_pct', MAX(percent_slower), 'models_affected', COUNT(*))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE 
         WHERE severity IN ('CRITICAL', 'HIGH')),
        OBJECT_CONSTRUCT('threshold_pct', 20),
        (SELECT ARRAY_AGG(model_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_PERFORMANCE 
         WHERE severity IN ('CRITICAL', 'HIGH'))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_MODEL_PERFORMANCE IS 
    'Triggers when models run >20% slower than baseline';

SELECT '✅ ALERT 6 CREATED: ALERT_MODEL_PERFORMANCE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 7: SLOW MODEL (Avg >5 minutes)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_SLOW_MODEL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 10 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
        WHERE avg_seconds > 300
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Slow Model Threshold Exceeded',
        'PERFORMANCE',
        CASE 
            WHEN (SELECT MAX(avg_seconds) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS WHERE avg_seconds > 300) > 600 THEN 'CRITICAL'
            ELSE 'HIGH'
        END,
        (SELECT LISTAGG(model_name || ': avg ' || ROUND(avg_seconds/60, 1) || ' min ($' || estimated_cost_usd || ')', ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
         WHERE avg_seconds > 300),
        (SELECT OBJECT_CONSTRUCT('slowest_model_seconds', MAX(avg_seconds), 'models_over_threshold', COUNT(*))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS
         WHERE avg_seconds > 300),
        OBJECT_CONSTRUCT('threshold_seconds', 300),
        (SELECT ARRAY_AGG(model_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_SLOWEST_MODELS 
         WHERE avg_seconds > 300)
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_SLOW_MODEL IS 
    'Triggers when any model averages more than 5 minutes execution';

SELECT '✅ ALERT 7 CREATED: ALERT_SLOW_MODEL' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 8: INCREMENTAL INEFFICIENCY (<1000 rows/second)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_INCREMENTAL_INEFFICIENCY
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 11 * * 1 UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
        WHERE efficiency_status LIKE '%INEFFICIENT%'
          AND load_strategy NOT IN ('VIEW (No Load)', 'UNKNOWN')
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Incremental Model Inefficiency',
        'PERFORMANCE',
        'MEDIUM',
        (SELECT LISTAGG(model_name || ' (' || load_strategy || '): ' || COALESCE(rows_per_second::VARCHAR, '0') || ' rows/sec', ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
         WHERE efficiency_status LIKE '%INEFFICIENT%'
           AND load_strategy NOT IN ('VIEW (No Load)', 'UNKNOWN')),
        (SELECT OBJECT_CONSTRUCT('min_rows_per_second', MIN(rows_per_second), 'models_affected', COUNT(*))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY
         WHERE efficiency_status LIKE '%INEFFICIENT%'
           AND load_strategy NOT IN ('VIEW (No Load)', 'UNKNOWN')),
        OBJECT_CONSTRUCT('threshold_rows_per_second', 1000),
        (SELECT ARRAY_AGG(model_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_INCREMENTAL_EFFICIENCY 
         WHERE efficiency_status LIKE '%INEFFICIENT%'
           AND load_strategy NOT IN ('VIEW (No Load)', 'UNKNOWN'))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_INCREMENTAL_INEFFICIENCY IS 
    'Weekly alert for incremental models processing less than 1000 rows/second';

SELECT '✅ ALERT 8 CREATED: ALERT_INCREMENTAL_INEFFICIENCY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 4: SCHEMA DRIFT ALERTS (Category 3)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 4: Creating Schema Drift Alerts' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 9: SCHEMA DRIFT DETECTED
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_SCHEMA_DRIFT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON */30 * * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SCHEMA_DRIFT
        WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
          AND detected_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Schema Drift Detected',
        'SCHEMA',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SCHEMA_DRIFT
         WHERE detected_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())),
        (SELECT LISTAGG(affected_object || ': ' || change_description, ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SCHEMA_DRIFT
         WHERE detected_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())),
        (SELECT OBJECT_CONSTRUCT('changes_detected', COUNT(*))
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SCHEMA_DRIFT
         WHERE detected_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())),
        OBJECT_CONSTRUCT('monitoring_window_hours', 1),
        (SELECT ARRAY_AGG(DISTINCT affected_object) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SCHEMA_DRIFT
         WHERE detected_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP()))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_SCHEMA_DRIFT IS 
    'Triggers when DDL changes or schema modifications are detected';

SELECT '✅ ALERT 9 CREATED: ALERT_SCHEMA_DRIFT' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 5: DBT OBSERVABILITY ALERTS (Category 7)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 5: Creating dbt Observability Alerts' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 10: DBT TEST COVERAGE GAPS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_DBT_COVERAGE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 11 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DBT_COVERAGE
        WHERE alert_type IN ('NO_TESTS', 'FAILING_TESTS')
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'dbt Test Coverage Issues',
        'DBT',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DBT_COVERAGE
         WHERE alert_type IN ('NO_TESTS', 'FAILING_TESTS')),
        (SELECT LISTAGG(alert_type || ': ' || model_name, ', ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DBT_COVERAGE
         WHERE alert_type IN ('NO_TESTS', 'FAILING_TESTS')
         LIMIT 10),
        (SELECT OBJECT_CONSTRUCT(
            'models_without_tests', SUM(CASE WHEN alert_type = 'NO_TESTS' THEN 1 ELSE 0 END),
            'models_with_failures', SUM(CASE WHEN alert_type = 'FAILING_TESTS' THEN 1 ELSE 0 END)
         )
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DBT_COVERAGE
         WHERE alert_type IN ('NO_TESTS', 'FAILING_TESTS')),
        OBJECT_CONSTRUCT('min_tests_per_model', 1),
        (SELECT ARRAY_AGG(model_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DBT_COVERAGE
         WHERE alert_type IN ('NO_TESTS', 'FAILING_TESTS'))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_DBT_COVERAGE IS 
    'Daily alert for models without tests or with failing tests';

SELECT '✅ ALERT 10 CREATED: ALERT_DBT_COVERAGE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 6: DATA INTEGRITY ALERTS (Category 8)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 6: Creating Data Integrity Alerts' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT 11: DATA INTEGRITY ISSUES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT O2C_AUDIT.ALERT_DATA_INTEGRITY
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
IF (
    EXISTS (
        SELECT 1 
        FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
        WHERE severity IN ('CRITICAL', 'HIGH')
    )
)
THEN
    CALL O2C_AUDIT.LOG_O2C_ALERT(
        'Data Integrity Issues Detected',
        'INTEGRITY',
        (SELECT MAX(severity) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
         WHERE severity IN ('CRITICAL', 'HIGH')),
        (SELECT LISTAGG(alert_type || ': ' || table_name || ' - ' || LEFT(alert_description, 100), '; ')
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
         WHERE severity IN ('CRITICAL', 'HIGH')
         LIMIT 5),
        (SELECT OBJECT_CONSTRUCT(
            'pk_issues', SUM(CASE WHEN alert_type = 'PK_VIOLATION' THEN 1 ELSE 0 END),
            'fk_issues', SUM(CASE WHEN alert_type = 'FK_VIOLATION' THEN 1 ELSE 0 END),
            'duplicate_issues', SUM(CASE WHEN alert_type = 'DUPLICATES' THEN 1 ELSE 0 END),
            'null_issues', SUM(CASE WHEN alert_type = 'HIGH_NULL_RATE' THEN 1 ELSE 0 END),
            'consistency_issues', SUM(CASE WHEN alert_type = 'CONSISTENCY_ISSUE' THEN 1 ELSE 0 END)
         )
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
         WHERE severity IN ('CRITICAL', 'HIGH')),
        OBJECT_CONSTRUCT('expected_issues', 0),
        (SELECT ARRAY_AGG(DISTINCT table_name) 
         FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_DATA_INTEGRITY
         WHERE severity IN ('CRITICAL', 'HIGH'))
    );

COMMENT ON ALERT O2C_AUDIT.ALERT_DATA_INTEGRITY IS 
    'Daily alert for PK, FK, duplicate, and consistency issues';

SELECT '✅ ALERT 11 CREATED: ALERT_DATA_INTEGRITY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: ENABLE ALL ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 Enabling all alerts...' AS status;

ALTER ALERT O2C_AUDIT.ALERT_COST_SPIKE RESUME;
ALTER ALERT O2C_AUDIT.ALERT_HIGH_COST_MODEL RESUME;
ALTER ALERT O2C_AUDIT.ALERT_MONTHLY_BUDGET RESUME;
ALTER ALERT O2C_AUDIT.ALERT_QUEUE_TIME RESUME;
ALTER ALERT O2C_AUDIT.ALERT_LONG_RUNNING_QUERY RESUME;
ALTER ALERT O2C_AUDIT.ALERT_MODEL_PERFORMANCE RESUME;
ALTER ALERT O2C_AUDIT.ALERT_SLOW_MODEL RESUME;
ALTER ALERT O2C_AUDIT.ALERT_INCREMENTAL_INEFFICIENCY RESUME;
ALTER ALERT O2C_AUDIT.ALERT_SCHEMA_DRIFT RESUME;
ALTER ALERT O2C_AUDIT.ALERT_DBT_COVERAGE RESUME;
ALTER ALERT O2C_AUDIT.ALERT_DATA_INTEGRITY RESUME;

SELECT '✅ All alerts enabled' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: CREATE ACTIVE ALERTS VIEW (for MC consumption)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_AUDIT.V_ACTIVE_ALERTS AS
SELECT 
    alert_id,
    alert_name,
    alert_type,
    severity,
    alert_message,
    affected_objects,
    triggered_at,
    acknowledged_at,
    resolved_at,
    CASE 
        WHEN resolved_at IS NOT NULL THEN 'RESOLVED'
        WHEN acknowledged_at IS NOT NULL THEN 'ACKNOWLEDGED'
        ELSE 'OPEN'
    END AS status
FROM O2C_AUDIT.O2C_ALERT_HISTORY
WHERE resolved_at IS NULL
  AND triggered_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    triggered_at DESC;

COMMENT ON VIEW O2C_AUDIT.V_ACTIVE_ALERTS IS 
    'Active (unresolved) alerts for monitoring dashboards and MC consumption';

SELECT '✅ STEP 5: Active alerts view created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6: GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

GRANT SELECT ON O2C_AUDIT.O2C_ALERT_HISTORY TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON O2C_AUDIT.V_ACTIVE_ALERTS TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE O2C_AUDIT.LOG_O2C_ALERT(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARIANT, VARIANT, ARRAY) TO ROLE DBT_O2C_DEVELOPER;

SELECT '✅ STEP 6: Permissions granted' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ NATIVE SNOWFLAKE ALERTS SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Show all alerts
SHOW ALERTS IN SCHEMA O2C_AUDIT;

-- Summary of alerts
SELECT 
    'ALERT_COST_SPIKE' AS alert_name, 'Cost' AS category, 'Daily cost >50% above 7-day avg' AS trigger_condition, '8am & 6pm UTC' AS schedule
UNION ALL SELECT 'ALERT_HIGH_COST_MODEL', 'Cost', 'Single model >$5/week', 'Daily 9am UTC'
UNION ALL SELECT 'ALERT_MONTHLY_BUDGET', 'Cost', 'Monthly cost >$100', 'Daily 9am UTC'
UNION ALL SELECT 'ALERT_QUEUE_TIME', 'Query', 'Avg queue >10 seconds', 'Every 30 min'
UNION ALL SELECT 'ALERT_LONG_RUNNING_QUERY', 'Query', 'Query >5 minutes', 'Every 15 min'
UNION ALL SELECT 'ALERT_MODEL_PERFORMANCE', 'Model', '>20% slower than baseline', 'Every 4 hours'
UNION ALL SELECT 'ALERT_SLOW_MODEL', 'Model', 'Avg execution >5 minutes', 'Daily 10am UTC'
UNION ALL SELECT 'ALERT_INCREMENTAL_INEFFICIENCY', 'Model', '<1000 rows/second', 'Weekly Monday 11am UTC'
UNION ALL SELECT 'ALERT_SCHEMA_DRIFT', 'Schema', 'DDL changes detected', 'Every 30 min'
UNION ALL SELECT 'ALERT_DBT_COVERAGE', 'dbt', 'Models without tests', 'Daily 11am UTC'
UNION ALL SELECT 'ALERT_DATA_INTEGRITY', 'Integrity', 'PK/FK/Duplicate issues', 'Daily 8am UTC';

