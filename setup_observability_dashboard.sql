-- ============================================================================
-- DBT Observability Dashboard Setup
-- ============================================================================
-- Purpose: Create monitoring views for Snowsight dashboards
-- Packages: dbt_artifacts + dbt_snowflake_monitoring
-- ============================================================================

-- PREREQUISITES:
-- 1. Install packages: dbt deps
-- 2. Run dbt at least once: dbt run
-- 3. Verify artifact tables exist:
--    - DBT_ARTIFACTS.MODEL_EXECUTIONS
--    - DBT_ARTIFACTS.TEST_EXECUTIONS
--    - SNOWFLAKE_MONITORING.QUERY_HISTORY (if using dbt_snowflake_monitoring)
-- ============================================================================

-- STEP 1: Create Monitoring Schema
-- ============================================================================
USE ROLE ACCOUNTADMIN; -- Or appropriate role with CREATE SCHEMA privileges
USE DATABASE EDW;

CREATE SCHEMA IF NOT EXISTS DBT_MONITORING
    COMMENT = 'DBT observability views for Snowsight dashboards';

USE SCHEMA DBT_MONITORING;

-- ============================================================================
-- STEP 2: Create Monitoring Views
-- ============================================================================

-- View 1: Daily DBT Execution Summary
-- ============================================================================
CREATE OR REPLACE VIEW DAILY_EXECUTION_SUMMARY AS
SELECT 
    DATE_TRUNC('day', generated_at) as execution_date,
    COUNT(DISTINCT node_id) as models_run,
    COUNT(DISTINCT CASE WHEN status = 'success' THEN node_id END) as successful_models,
    COUNT(DISTINCT CASE WHEN status = 'error' THEN node_id END) as failed_models,
    SUM(execution_time) as total_execution_seconds,
    AVG(execution_time) as avg_execution_seconds,
    MAX(execution_time) as max_execution_seconds
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE generated_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

COMMENT ON VIEW DAILY_EXECUTION_SUMMARY IS 
    'Daily summary of dbt model executions for trend analysis';

-- View 2: Model Performance Ranking
-- ============================================================================
CREATE OR REPLACE VIEW MODEL_PERFORMANCE_RANKING AS
SELECT 
    node_id,
    SPLIT_PART(node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    AVG(execution_time) as avg_execution_seconds,
    MAX(execution_time) as max_execution_seconds,
    MIN(execution_time) as min_execution_seconds,
    STDDEV(execution_time) as stddev_execution_seconds,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed_runs
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY 1, 2
HAVING run_count > 0
ORDER BY avg_execution_seconds DESC;

COMMENT ON VIEW MODEL_PERFORMANCE_RANKING IS 
    'Performance metrics per dbt model for optimization prioritization';

-- View 3: Test Results Health
-- ============================================================================
CREATE OR REPLACE VIEW TEST_RESULTS_HEALTH AS
SELECT 
    DATE_TRUNC('day', generated_at) as test_date,
    status,
    COUNT(*) as test_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE_TRUNC('day', generated_at)) as percentage
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE generated_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

COMMENT ON VIEW TEST_RESULTS_HEALTH IS 
    'Daily test pass/fail rates for data quality monitoring';

-- View 4: Failed Tests Detail
-- ============================================================================
CREATE OR REPLACE VIEW FAILED_TESTS_DETAIL AS
SELECT 
    generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as test_name,
    status,
    execution_time,
    message
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND generated_at >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY generated_at DESC;

COMMENT ON VIEW FAILED_TESTS_DETAIL IS 
    'Recent test failures for investigation and alerting';

-- View 5: Model Execution Trends (7-day moving average)
-- ============================================================================
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
        DATE_TRUNC('day', generated_at) as execution_date,
        SPLIT_PART(node_id, '.', -1) as model_name,
        AVG(execution_time) as avg_execution_seconds
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1, 2
)
ORDER BY execution_date DESC, model_name;

COMMENT ON VIEW MODEL_EXECUTION_TRENDS IS 
    'Model execution time trends with moving averages for anomaly detection';

-- View 6: Slowest Models (Current Week)
-- ============================================================================
CREATE OR REPLACE VIEW SLOWEST_MODELS_CURRENT_WEEK AS
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    node_id,
    COUNT(*) as run_count,
    AVG(execution_time) as avg_seconds,
    MAX(execution_time) as max_seconds,
    SUM(execution_time) as total_seconds,
    -- Classify performance
    CASE 
        WHEN AVG(execution_time) > 300 THEN 'CRITICAL'
        WHEN AVG(execution_time) > 60 THEN 'SLOW'
        WHEN AVG(execution_time) > 10 THEN 'MODERATE'
        ELSE 'FAST'
    END as performance_tier
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'success'
GROUP BY 1, 2
ORDER BY avg_seconds DESC
LIMIT 20;

COMMENT ON VIEW SLOWEST_MODELS_CURRENT_WEEK IS 
    'Top 20 slowest models for optimization priority';

-- View 7: Source Freshness Status
-- ============================================================================
CREATE OR REPLACE VIEW SOURCE_FRESHNESS_STATUS AS
SELECT 
    DATE_TRUNC('day', generated_at) as check_date,
    node_id,
    SPLIT_PART(node_id, '.', -1) as source_name,
    status,
    max_loaded_at,
    snapshotted_at,
    DATEDIFF('hour', max_loaded_at, snapshotted_at) as hours_since_last_load
FROM DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY generated_at DESC;

COMMENT ON VIEW SOURCE_FRESHNESS_STATUS IS 
    'Source freshness check results for data staleness monitoring';

-- ============================================================================
-- OPTIONAL: Snowflake Query History Integration
-- ============================================================================
-- If you have dbt_snowflake_monitoring installed, create cost views:

-- View 8: DBT Query Costs (Requires dbt_snowflake_monitoring)
-- ============================================================================
CREATE OR REPLACE VIEW DBT_QUERY_COSTS AS
SELECT 
    DATE_TRUNC('day', start_time) as query_date,
    query_type,
    COUNT(*) as query_count,
    SUM(execution_time)/1000/60 as total_minutes,
    SUM(credits_used) as total_credits,
    AVG(credits_used) as avg_credits_per_query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

COMMENT ON VIEW DBT_QUERY_COSTS IS 
    'Daily DBT query costs from Snowflake query history';

-- View 9: Cost by Model (Estimated - Requires warehouse metadata)
-- ============================================================================
CREATE OR REPLACE VIEW COST_BY_MODEL_ESTIMATED AS
WITH warehouse_costs AS (
    -- Approximate warehouse costs per second
    -- Adjust these based on your Snowflake pricing
    SELECT 'COMPUTE_WH' as warehouse_name, 0.0006 as cost_per_second
)
SELECT 
    m.node_id,
    SPLIT_PART(m.node_id, '.', -1) as model_name,
    COUNT(*) as run_count,
    SUM(m.execution_time) as total_execution_seconds,
    SUM(m.execution_time * w.cost_per_second) as estimated_cost_usd,
    AVG(m.execution_time * w.cost_per_second) as avg_cost_per_run_usd
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m
CROSS JOIN warehouse_costs w
WHERE m.generated_at >= DATEADD(day, -7, CURRENT_DATE())
  AND m.status = 'success'
GROUP BY 1, 2
ORDER BY estimated_cost_usd DESC
LIMIT 20;

COMMENT ON VIEW COST_BY_MODEL_ESTIMATED IS 
    'Estimated cost per dbt model (requires warehouse cost assumptions)';

-- ============================================================================
-- STEP 3: Create Alert Views (For Task-Based Alerting)
-- ============================================================================

-- Alert View 1: Models Running Slower Than Usual
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_SLOW_MODELS AS
WITH model_baseline AS (
    SELECT 
        node_id,
        AVG(execution_time) as baseline_avg,
        STDDEV(execution_time) as baseline_stddev
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) AND DATEADD(day, -7, CURRENT_DATE())
    GROUP BY node_id
),
recent_runs AS (
    SELECT 
        node_id,
        AVG(execution_time) as recent_avg
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -1, CURRENT_DATE())
    GROUP BY node_id
)
SELECT 
    r.node_id,
    SPLIT_PART(r.node_id, '.', -1) as model_name,
    b.baseline_avg,
    r.recent_avg,
    r.recent_avg - b.baseline_avg as seconds_slower,
    (r.recent_avg - b.baseline_avg) / b.baseline_avg * 100 as percent_slower
FROM recent_runs r
JOIN model_baseline b ON r.node_id = b.node_id
WHERE r.recent_avg > b.baseline_avg + (2 * b.baseline_stddev) -- 2 standard deviations
ORDER BY percent_slower DESC;

COMMENT ON VIEW ALERT_SLOW_MODELS IS 
    'Models running significantly slower than baseline (2 sigma threshold)';

-- Alert View 2: Recent Test Failures
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_RECENT_TEST_FAILURES AS
SELECT 
    generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as test_name,
    status,
    message,
    execution_time
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND generated_at >= DATEADD(hour, -24, CURRENT_DATE())
ORDER BY generated_at DESC;

COMMENT ON VIEW ALERT_RECENT_TEST_FAILURES IS 
    'Test failures in last 24 hours for immediate alerting';

-- ============================================================================
-- STEP 4: Grant Permissions
-- ============================================================================

-- Grant read access to analysts/BI users
GRANT USAGE ON SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;

-- Grant future permissions
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DBT_MONITORING TO ROLE DBT_DEV_ROLE;

-- ============================================================================
-- STEP 5: Verification Queries
-- ============================================================================

-- Test all views are working
SELECT 'DAILY_EXECUTION_SUMMARY' as view_name, COUNT(*) as row_count FROM DAILY_EXECUTION_SUMMARY
UNION ALL
SELECT 'MODEL_PERFORMANCE_RANKING', COUNT(*) FROM MODEL_PERFORMANCE_RANKING
UNION ALL
SELECT 'TEST_RESULTS_HEALTH', COUNT(*) FROM TEST_RESULTS_HEALTH
UNION ALL
SELECT 'FAILED_TESTS_DETAIL', COUNT(*) FROM FAILED_TESTS_DETAIL
UNION ALL
SELECT 'MODEL_EXECUTION_TRENDS', COUNT(*) FROM MODEL_EXECUTION_TRENDS
UNION ALL
SELECT 'SLOWEST_MODELS_CURRENT_WEEK', COUNT(*) FROM SLOWEST_MODELS_CURRENT_WEEK
UNION ALL
SELECT 'SOURCE_FRESHNESS_STATUS', COUNT(*) FROM SOURCE_FRESHNESS_STATUS
UNION ALL
SELECT 'DBT_QUERY_COSTS', COUNT(*) FROM DBT_QUERY_COSTS
UNION ALL
SELECT 'COST_BY_MODEL_ESTIMATED', COUNT(*) FROM COST_BY_MODEL_ESTIMATED
UNION ALL
SELECT 'ALERT_SLOW_MODELS', COUNT(*) FROM ALERT_SLOW_MODELS
UNION ALL
SELECT 'ALERT_RECENT_TEST_FAILURES', COUNT(*) FROM ALERT_RECENT_TEST_FAILURES;

-- ============================================================================
-- STEP 6: Build Snowsight Dashboard
-- ============================================================================

/*
NOW GO TO SNOWSIGHT:

1. Click "Dashboards" → "New Dashboard"
2. Name: "DBT Observability"

ADD THESE TILES:

Tile 1: Daily Execution Summary
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY
Chart: Line chart
Metrics: models_run, total_execution_seconds
X-axis: execution_date

Tile 2: Test Pass Rate
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.TEST_RESULTS_HEALTH
Chart: Stacked area chart
Metrics: test_count
Group by: status
X-axis: test_date

Tile 3: Top 10 Slowest Models
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK LIMIT 10
Chart: Bar chart
X-axis: model_name
Y-axis: avg_seconds

Tile 4: Model Performance Trends
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.MODEL_EXECUTION_TRENDS 
       WHERE model_name IN ('dm_fin_ar_aging_simple', 'dim_customer', 'dim_fiscal_calendar')
Chart: Multi-line chart
Metrics: avg_execution_seconds, moving_avg_7day
X-axis: execution_date
Group by: model_name

Tile 5: Daily Costs
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.DBT_QUERY_COSTS
Chart: Line chart
Metrics: total_credits
X-axis: query_date

Tile 6: Failed Tests Alert
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.ALERT_RECENT_TEST_FAILURES
Chart: Table
Columns: test_name, status, generated_at
Alert: Show when count > 0

Tile 7: Performance Alerts
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.ALERT_SLOW_MODELS
Chart: Table
Columns: model_name, percent_slower, recent_avg
Alert: Show when count > 0

Tile 8: Source Freshness
─────────────────────────────
Query: SELECT * FROM DBT_MONITORING.SOURCE_FRESHNESS_STATUS
       WHERE status = 'error' OR hours_since_last_load > 24
Chart: Table
Alert: Show when count > 0

3. Set refresh schedule: Hourly or Daily
4. Share with team
5. Set up email alerts for critical tiles
*/

-- ============================================================================
-- OPTIONAL: Create Alerting Task
-- ============================================================================

-- Task to send email if tests fail
CREATE OR REPLACE TASK DBT_MONITORING.ALERT_ON_TEST_FAILURES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */4 * * * America/New_York' -- Every 4 hours
AS
DECLARE
    failure_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO :failure_count 
    FROM DBT_MONITORING.ALERT_RECENT_TEST_FAILURES;
    
    IF (:failure_count > 0) THEN
        -- Send notification (configure with your email)
        CALL SYSTEM$SEND_EMAIL(
            'dbt_alerts',
            'data-team@company.com',
            'DBT Test Failures Detected',
            'There are ' || :failure_count || ' test failures in the last 24 hours. Check the DBT Observability dashboard.'
        );
    END IF;
END;

-- Enable the task
-- ALTER TASK DBT_MONITORING.ALERT_ON_TEST_FAILURES RESUME;

-- ============================================================================
-- END OF SETUP
-- ============================================================================

-- Summary of created objects:
SELECT 'Setup complete!' as status,
       'Views created: 11' as views,
       'Alert queries: 2' as alerts,
       'Next: Build Snowsight dashboard' as next_step;

