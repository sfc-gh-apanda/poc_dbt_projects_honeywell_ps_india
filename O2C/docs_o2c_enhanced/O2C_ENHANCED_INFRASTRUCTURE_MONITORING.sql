-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - INFRASTRUCTURE, SECURITY & OPERATIONAL MONITORING
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Warehouse, Storage, Security, Task/Stream, and Concurrency monitoring
-- 
-- Views Created (25 total):
--   WAREHOUSE & RESOURCE MONITORING (Category 4):
--     1. O2C_ENH_WAREHOUSE_UTILIZATION     - Warehouse usage patterns
--     2. O2C_ENH_WAREHOUSE_CREDITS         - Credit consumption by warehouse
--     3. O2C_ENH_WAREHOUSE_CONCURRENCY     - Concurrent query analysis
--     4. O2C_ENH_WAREHOUSE_SCALING         - Multi-cluster scaling events
--     5. O2C_ENH_ALERT_WAREHOUSE           - Warehouse alerts
--
--   SECURITY & ACCESS MONITORING (Category 5):
--     6. O2C_ENH_LOGIN_HISTORY             - Login activity analysis
--     7. O2C_ENH_ACCESS_PATTERNS           - Data access patterns
--     8. O2C_ENH_ROLE_USAGE                - Role usage analysis
--     9. O2C_ENH_FAILED_LOGINS             - Failed login attempts
--    10. O2C_ENH_ALERT_SECURITY            - Security alerts
--
--   STORAGE MONITORING (Category 6):
--    11. O2C_ENH_STORAGE_USAGE             - Current storage by object
--    12. O2C_ENH_STORAGE_GROWTH            - Storage growth trends
--    13. O2C_ENH_TABLE_SIZES               - Table size rankings
--    14. O2C_ENH_STORAGE_FORECAST          - Storage cost projection
--    15. O2C_ENH_ALERT_STORAGE             - Storage alerts
--
--   TASK & STREAM MONITORING (Category 9):
--    16. O2C_ENH_TASK_HISTORY              - Task execution history
--    17. O2C_ENH_TASK_PERFORMANCE          - Task performance metrics
--    18. O2C_ENH_STREAM_LAG                - Stream lag analysis
--    19. O2C_ENH_TASK_DEPENDENCIES         - Task dependency graph
--    20. O2C_ENH_ALERT_TASKS               - Task/Stream alerts
--
--   CONCURRENCY & CONTENTION (Category 10):
--    21. O2C_ENH_QUERY_CONCURRENCY         - Concurrent query analysis
--    22. O2C_ENH_BLOCKED_QUERIES           - Blocked/waiting queries
--    23. O2C_ENH_LOCK_WAITS                - Lock wait analysis
--    24. O2C_ENH_SPILL_ANALYSIS            - Disk spill analysis
--    25. O2C_ENH_ALERT_CONTENTION          - Contention alerts
--
-- Prerequisites:
--   - O2C_ENHANCED_MONITORING_SETUP.sql executed
--   - SNOWFLAKE.ACCOUNT_USAGE access granted
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_ENHANCED_MONITORING;

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '🚀 STARTING: Infrastructure, Security & Operational Monitoring' AS status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 1: WAREHOUSE & RESOURCE MONITORING (Category 4)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 1: Warehouse & Resource Monitoring' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 1: WAREHOUSE UTILIZATION
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_WAREHOUSE_UTILIZATION AS
SELECT
    DATE_TRUNC('hour', start_time) AS hour_bucket,
    warehouse_name,
    warehouse_size,
    COUNT(*) AS query_count,
    COUNT(DISTINCT user_name) AS unique_users,
    ROUND(SUM(total_elapsed_time) / 1000 / 60, 2) AS total_execution_minutes,
    ROUND(AVG(total_elapsed_time) / 1000, 2) AS avg_query_seconds,
    ROUND(MAX(total_elapsed_time) / 1000, 2) AS max_query_seconds,
    ROUND(SUM(bytes_scanned) / 1024 / 1024 / 1024, 2) AS gb_scanned,
    ROUND(SUM(bytes_written) / 1024 / 1024 / 1024, 2) AS gb_written,
    -- Utilization metrics
    ROUND(SUM(total_elapsed_time) / 1000 / 3600, 4) AS compute_hours,
    -- Percentage of hour utilized (assuming single cluster)
    ROUND(SUM(total_elapsed_time) / 1000 / 3600 * 100, 2) AS utilization_pct,
    -- Status indicators
    CASE 
        WHEN SUM(total_elapsed_time) / 1000 / 3600 * 100 > 80 THEN '🔴 HIGH UTILIZATION'
        WHEN SUM(total_elapsed_time) / 1000 / 3600 * 100 > 50 THEN '🟡 MODERATE'
        WHEN SUM(total_elapsed_time) / 1000 / 3600 * 100 > 20 THEN '🟢 HEALTHY'
        ELSE '⚪ LOW (Consider sizing down)'
    END AS utilization_status,
    -- Recommendations
    CASE 
        WHEN COUNT(*) > 100 AND AVG(queued_overload_time) > 5000 THEN 'Scale up or add multi-cluster'
        WHEN COUNT(*) < 10 AND SUM(total_elapsed_time) / 1000 / 3600 * 100 < 10 THEN 'Consider sizing down'
        ELSE 'Size appropriate'
    END AS sizing_recommendation
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND warehouse_name IS NOT NULL
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
GROUP BY hour_bucket, warehouse_name, warehouse_size
ORDER BY hour_bucket DESC, warehouse_name;

COMMENT ON VIEW O2C_ENH_WAREHOUSE_UTILIZATION IS 
    'Hourly warehouse utilization analysis with sizing recommendations';

SELECT '✅ VIEW 1 CREATED: O2C_ENH_WAREHOUSE_UTILIZATION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 2: WAREHOUSE CREDITS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_WAREHOUSE_CREDITS AS
SELECT
    DATE(start_time) AS usage_date,
    warehouse_name,
    warehouse_size,
    -- Credit calculation based on warehouse size
    ROUND(SUM(
        CASE warehouse_size
            WHEN 'X-Small' THEN total_elapsed_time / 1000 / 3600 * 1
            WHEN 'Small' THEN total_elapsed_time / 1000 / 3600 * 2
            WHEN 'Medium' THEN total_elapsed_time / 1000 / 3600 * 4
            WHEN 'Large' THEN total_elapsed_time / 1000 / 3600 * 8
            WHEN 'X-Large' THEN total_elapsed_time / 1000 / 3600 * 16
            WHEN '2X-Large' THEN total_elapsed_time / 1000 / 3600 * 32
            WHEN '3X-Large' THEN total_elapsed_time / 1000 / 3600 * 64
            WHEN '4X-Large' THEN total_elapsed_time / 1000 / 3600 * 128
            ELSE total_elapsed_time / 1000 / 3600 * 1
        END
    ), 4) AS estimated_credits,
    COUNT(*) AS query_count,
    ROUND(SUM(bytes_scanned) / 1024 / 1024 / 1024, 2) AS gb_scanned,
    -- Cost at $3/credit
    ROUND(SUM(
        CASE warehouse_size
            WHEN 'X-Small' THEN total_elapsed_time / 1000 / 3600 * 1
            WHEN 'Small' THEN total_elapsed_time / 1000 / 3600 * 2
            WHEN 'Medium' THEN total_elapsed_time / 1000 / 3600 * 4
            WHEN 'Large' THEN total_elapsed_time / 1000 / 3600 * 8
            WHEN 'X-Large' THEN total_elapsed_time / 1000 / 3600 * 16
            WHEN '2X-Large' THEN total_elapsed_time / 1000 / 3600 * 32
            ELSE total_elapsed_time / 1000 / 3600 * 1
        END
    ) * 3.0, 2) AS estimated_cost_usd,
    -- 7-day average
    AVG(SUM(
        CASE warehouse_size
            WHEN 'X-Small' THEN total_elapsed_time / 1000 / 3600 * 1
            WHEN 'Small' THEN total_elapsed_time / 1000 / 3600 * 2
            WHEN 'Medium' THEN total_elapsed_time / 1000 / 3600 * 4
            WHEN 'Large' THEN total_elapsed_time / 1000 / 3600 * 8
            ELSE total_elapsed_time / 1000 / 3600 * 1
        END
    )) OVER (
        PARTITION BY warehouse_name 
        ORDER BY DATE(start_time) 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS credits_7day_avg
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND warehouse_name IS NOT NULL
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
GROUP BY usage_date, warehouse_name, warehouse_size
ORDER BY usage_date DESC, estimated_credits DESC;

COMMENT ON VIEW O2C_ENH_WAREHOUSE_CREDITS IS 
    'Daily warehouse credit consumption with 7-day averages';

SELECT '✅ VIEW 2 CREATED: O2C_ENH_WAREHOUSE_CREDITS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 3: WAREHOUSE CONCURRENCY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_WAREHOUSE_CONCURRENCY AS
WITH time_buckets AS (
    SELECT
        DATE_TRUNC('minute', start_time) AS minute_bucket,
        warehouse_name,
        COUNT(*) AS concurrent_queries,
        SUM(CASE WHEN queued_overload_time > 0 THEN 1 ELSE 0 END) AS queued_queries,
        AVG(queued_overload_time) / 1000 AS avg_queue_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND warehouse_name IS NOT NULL
      AND start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY minute_bucket, warehouse_name
)
SELECT
    DATE(minute_bucket) AS date,
    HOUR(minute_bucket) AS hour,
    warehouse_name,
    MAX(concurrent_queries) AS peak_concurrent,
    AVG(concurrent_queries) AS avg_concurrent,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY concurrent_queries), 0) AS p95_concurrent,
    SUM(queued_queries) AS total_queued,
    ROUND(AVG(avg_queue_seconds), 2) AS avg_queue_seconds,
    CASE 
        WHEN MAX(concurrent_queries) > 50 THEN '🔴 HIGH CONCURRENCY'
        WHEN MAX(concurrent_queries) > 20 THEN '🟡 MODERATE'
        WHEN MAX(concurrent_queries) > 5 THEN '🟢 NORMAL'
        ELSE '⚪ LOW'
    END AS concurrency_status,
    CASE 
        WHEN SUM(queued_queries) > 0 AND AVG(avg_queue_seconds) > 10 THEN 'Consider multi-cluster or larger warehouse'
        ELSE 'Concurrency adequate'
    END AS recommendation
FROM time_buckets
GROUP BY date, hour, warehouse_name
ORDER BY date DESC, hour DESC, warehouse_name;

COMMENT ON VIEW O2C_ENH_WAREHOUSE_CONCURRENCY IS 
    'Warehouse concurrency analysis with peak and queue metrics';

SELECT '✅ VIEW 3 CREATED: O2C_ENH_WAREHOUSE_CONCURRENCY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 4: WAREHOUSE SCALING EVENTS (Multi-cluster)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_WAREHOUSE_SCALING AS
SELECT
    timestamp AS event_time,
    warehouse_name,
    cluster_number,
    event_name,
    event_reason,
    event_state,
    CASE 
        WHEN event_name = 'SCALE_UP' THEN '⬆️ SCALE UP'
        WHEN event_name = 'SCALE_DOWN' THEN '⬇️ SCALE DOWN'
        WHEN event_name = 'SUSPEND' THEN '⏸️ SUSPENDED'
        WHEN event_name = 'RESUME' THEN '▶️ RESUMED'
        ELSE event_name
    END AS event_display,
    CASE 
        WHEN event_reason = 'QUERY_QUEUED' THEN 'Queries were queued'
        WHEN event_reason = 'IDLE' THEN 'Warehouse idle'
        WHEN event_reason = 'USER' THEN 'User initiated'
        ELSE event_reason
    END AS reason_description
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY
WHERE timestamp >= DATEADD('day', -14, CURRENT_DATE())
ORDER BY timestamp DESC;

COMMENT ON VIEW O2C_ENH_WAREHOUSE_SCALING IS 
    'Multi-cluster warehouse scaling events history';

SELECT '✅ VIEW 4 CREATED: O2C_ENH_WAREHOUSE_SCALING' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 5: WAREHOUSE ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_WAREHOUSE AS
-- High utilization alerts
SELECT
    'HIGH_UTILIZATION' AS alert_type,
    hour_bucket AS detected_at,
    warehouse_name,
    utilization_pct AS metric_value,
    'Warehouse utilization: ' || utilization_pct || '%' AS alert_description,
    CASE 
        WHEN utilization_pct > 90 THEN 'CRITICAL'
        WHEN utilization_pct > 80 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    sizing_recommendation AS recommendation
FROM O2C_ENH_WAREHOUSE_UTILIZATION
WHERE hour_bucket >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
  AND utilization_pct > 75

UNION ALL

-- High queue time alerts
SELECT
    'HIGH_QUEUE_TIME',
    DATE_TRUNC('hour', CURRENT_TIMESTAMP()),
    warehouse_name,
    avg_queue_seconds,
    'Avg queue time: ' || avg_queue_seconds || 's with ' || total_queued || ' queries queued',
    CASE 
        WHEN avg_queue_seconds > 30 THEN 'CRITICAL'
        WHEN avg_queue_seconds > 15 THEN 'HIGH'
        ELSE 'MEDIUM'
    END,
    recommendation
FROM O2C_ENH_WAREHOUSE_CONCURRENCY
WHERE date >= CURRENT_DATE() - 1
  AND avg_queue_seconds > 10

UNION ALL

-- Low utilization (cost saving opportunity)
SELECT
    'LOW_UTILIZATION',
    hour_bucket,
    warehouse_name,
    utilization_pct,
    'Warehouse underutilized: ' || utilization_pct || '% - consider downsizing',
    'LOW',
    sizing_recommendation
FROM O2C_ENH_WAREHOUSE_UTILIZATION
WHERE hour_bucket >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND utilization_pct < 10
  AND query_count > 10  -- Has activity but low utilization

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_WAREHOUSE IS 
    'Warehouse performance and utilization alerts';

SELECT '✅ VIEW 5 CREATED: O2C_ENH_ALERT_WAREHOUSE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 2: SECURITY & ACCESS MONITORING (Category 5)
-- ███████████████████████████████████████████████████████████████████████████════

SELECT '📋 SECTION 2: Security & Access Monitoring' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 6: LOGIN HISTORY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_LOGIN_HISTORY AS
SELECT
    DATE(event_timestamp) AS login_date,
    user_name,
    reported_client_type AS client_type,
    first_authentication_factor AS auth_method,
    is_success,
    COUNT(*) AS login_count,
    COUNT(CASE WHEN is_success = 'YES' THEN 1 END) AS successful_logins,
    COUNT(CASE WHEN is_success = 'NO' THEN 1 END) AS failed_logins,
    ROUND(COUNT(CASE WHEN is_success = 'NO' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS failure_rate_pct,
    MIN(event_timestamp) AS first_login,
    MAX(event_timestamp) AS last_login,
    LISTAGG(DISTINCT client_ip, ', ') WITHIN GROUP (ORDER BY client_ip) AS ip_addresses,
    CASE 
        WHEN COUNT(CASE WHEN is_success = 'NO' THEN 1 END) > 5 THEN '🔴 HIGH FAILURES'
        WHEN COUNT(CASE WHEN is_success = 'NO' THEN 1 END) > 0 THEN '🟡 HAS FAILURES'
        ELSE '🟢 ALL SUCCESSFUL'
    END AS login_status
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY login_date, user_name, client_type, auth_method, is_success
ORDER BY login_date DESC, failed_logins DESC;

COMMENT ON VIEW O2C_ENH_LOGIN_HISTORY IS 
    'User login history with success/failure analysis';

SELECT '✅ VIEW 6 CREATED: O2C_ENH_LOGIN_HISTORY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 7: DATA ACCESS PATTERNS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ACCESS_PATTERNS AS
SELECT
    DATE(start_time) AS access_date,
    user_name,
    role_name,
    database_name,
    schema_name,
    COUNT(*) AS query_count,
    COUNT(DISTINCT query_type) AS query_type_variety,
    LISTAGG(DISTINCT query_type, ', ') WITHIN GROUP (ORDER BY query_type) AS query_types_used,
    ROUND(SUM(bytes_scanned) / 1024 / 1024 / 1024, 2) AS gb_scanned,
    ROUND(SUM(bytes_written) / 1024 / 1024 / 1024, 2) AS gb_written,
    MIN(start_time) AS first_access,
    MAX(start_time) AS last_access,
    -- Access patterns
    CASE 
        WHEN COUNT(CASE WHEN query_type LIKE '%DELETE%' OR query_type LIKE '%DROP%' THEN 1 END) > 0 
        THEN '🔴 DESTRUCTIVE OPS'
        WHEN COUNT(CASE WHEN query_type LIKE '%UPDATE%' OR query_type LIKE '%INSERT%' OR query_type LIKE '%MERGE%' THEN 1 END) > 0 
        THEN '🟡 WRITE OPS'
        ELSE '🟢 READ ONLY'
    END AS access_pattern,
    -- Unusual activity flag
    CASE 
        WHEN HOUR(MIN(start_time)) < 6 OR HOUR(MAX(start_time)) > 22 THEN 'Off-hours activity'
        WHEN COUNT(*) > 1000 THEN 'High volume'
        ELSE 'Normal'
    END AS activity_flag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY access_date, user_name, role_name, database_name, schema_name
ORDER BY access_date DESC, query_count DESC;

COMMENT ON VIEW O2C_ENH_ACCESS_PATTERNS IS 
    'Data access patterns by user and role with activity flags';

SELECT '✅ VIEW 7 CREATED: O2C_ENH_ACCESS_PATTERNS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 8: ROLE USAGE ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ROLE_USAGE AS
SELECT
    role_name,
    COUNT(DISTINCT user_name) AS unique_users,
    COUNT(DISTINCT DATE(start_time)) AS active_days,
    COUNT(*) AS total_queries,
    COUNT(DISTINCT database_name || '.' || schema_name) AS schemas_accessed,
    LISTAGG(DISTINCT database_name || '.' || schema_name, ', ') WITHIN GROUP (ORDER BY schema_name) AS schemas_list,
    ROUND(SUM(bytes_scanned) / 1024 / 1024 / 1024, 2) AS total_gb_scanned,
    MIN(start_time) AS first_activity,
    MAX(start_time) AS last_activity,
    -- Role activity status
    CASE 
        WHEN MAX(start_time) < DATEADD('day', -7, CURRENT_TIMESTAMP()) THEN '⚪ INACTIVE (>7 days)'
        WHEN MAX(start_time) < DATEADD('day', -1, CURRENT_TIMESTAMP()) THEN '🟡 LOW ACTIVITY'
        ELSE '🟢 ACTIVE'
    END AS activity_status,
    -- Permission level indicator
    CASE 
        WHEN role_name LIKE '%ADMIN%' OR role_name = 'ACCOUNTADMIN' THEN '🔴 ADMIN'
        WHEN role_name LIKE '%DEVELOPER%' OR role_name LIKE '%WRITE%' THEN '🟡 DEVELOPER'
        ELSE '🟢 READ-ONLY'
    END AS permission_level
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY role_name
ORDER BY total_queries DESC;

COMMENT ON VIEW O2C_ENH_ROLE_USAGE IS 
    'Role usage analysis with activity and permission indicators';

SELECT '✅ VIEW 8 CREATED: O2C_ENH_ROLE_USAGE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 9: FAILED LOGINS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_FAILED_LOGINS AS
SELECT
    event_timestamp,
    user_name,
    client_ip,
    reported_client_type AS client_type,
    first_authentication_factor AS auth_method,
    error_code,
    error_message,
    connection_representative,
    -- Time-based grouping for pattern detection
    DATE(event_timestamp) AS failure_date,
    HOUR(event_timestamp) AS failure_hour,
    -- Suspicious indicators
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY user_name, DATE(event_timestamp)) > 5 
        THEN '🔴 MULTIPLE FAILURES'
        WHEN COUNT(*) OVER (PARTITION BY client_ip, DATE(event_timestamp)) > 10 
        THEN '🔴 IP-BASED ATTACK'
        ELSE '🟡 SINGLE FAILURE'
    END AS threat_indicator,
    -- Recommendations
    CASE 
        WHEN error_message ILIKE '%invalid%password%' THEN 'Check password, consider reset'
        WHEN error_message ILIKE '%locked%' THEN 'Account locked, admin action needed'
        WHEN error_message ILIKE '%expired%' THEN 'Credentials expired, renew'
        ELSE 'Investigate error'
    END AS recommendation
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE is_success = 'NO'
  AND event_timestamp >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY event_timestamp DESC;

COMMENT ON VIEW O2C_ENH_FAILED_LOGINS IS 
    'Failed login attempts with threat indicators';

SELECT '✅ VIEW 9 CREATED: O2C_ENH_FAILED_LOGINS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 10: SECURITY ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_SECURITY AS
-- Multiple failed logins per user
SELECT
    'MULTIPLE_FAILED_LOGINS' AS alert_type,
    MAX(event_timestamp) AS detected_at,
    user_name AS affected_entity,
    COUNT(*) AS metric_value,
    'User had ' || COUNT(*) || ' failed login attempts' AS alert_description,
    CASE 
        WHEN COUNT(*) > 10 THEN 'CRITICAL'
        WHEN COUNT(*) > 5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE is_success = 'NO'
  AND event_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY user_name
HAVING COUNT(*) > 3

UNION ALL

-- Off-hours data access
SELECT
    'OFF_HOURS_ACCESS',
    MAX(start_time),
    user_name,
    COUNT(*),
    'User accessed data ' || COUNT(*) || ' times during off-hours (10PM-6AM)',
    'MEDIUM'
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND (HOUR(start_time) < 6 OR HOUR(start_time) > 22)
GROUP BY user_name
HAVING COUNT(*) > 5

UNION ALL

-- Destructive operations
SELECT
    'DESTRUCTIVE_OPERATION',
    start_time,
    user_name,
    1,
    'Executed ' || query_type || ' on ' || COALESCE(schema_name || '.' || 
        REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1), 'unknown object'),
    CASE 
        WHEN query_type LIKE '%DROP%' THEN 'CRITICAL'
        ELSE 'HIGH'
    END
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND query_type IN ('DROP', 'DELETE', 'TRUNCATE_TABLE')
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_SECURITY IS 
    'Security alerts for failed logins, off-hours access, and destructive operations';

SELECT '✅ VIEW 10 CREATED: O2C_ENH_ALERT_SECURITY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 3: STORAGE MONITORING (Category 6)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 3: Storage Monitoring' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 11: STORAGE USAGE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_STORAGE_USAGE AS
SELECT
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name,
    table_type,
    row_count,
    ROUND(bytes / 1024 / 1024, 2) AS size_mb,
    ROUND(bytes / 1024 / 1024 / 1024, 4) AS size_gb,
    created AS created_at,
    last_altered,
    -- Storage cost estimate ($23/TB/month)
    ROUND(bytes / 1024 / 1024 / 1024 / 1024 * 23, 4) AS monthly_cost_usd,
    -- Size tier
    CASE 
        WHEN bytes / 1024 / 1024 / 1024 > 10 THEN '🔴 LARGE (>10GB)'
        WHEN bytes / 1024 / 1024 / 1024 > 1 THEN '🟡 MEDIUM (1-10GB)'
        WHEN bytes / 1024 / 1024 > 100 THEN '🟢 SMALL (100MB-1GB)'
        ELSE '⚪ TINY (<100MB)'
    END AS size_tier,
    -- Rows per MB (density)
    CASE 
        WHEN bytes > 0 THEN ROUND(row_count / (bytes / 1024 / 1024), 0)
        ELSE 0
    END AS rows_per_mb
FROM EDW.INFORMATION_SCHEMA.TABLES
WHERE table_schema LIKE 'O2C_ENHANCED%'
ORDER BY bytes DESC NULLS LAST;

COMMENT ON VIEW O2C_ENH_STORAGE_USAGE IS 
    'Current storage usage by O2C Enhanced table with cost estimates';

SELECT '✅ VIEW 11 CREATED: O2C_ENH_STORAGE_USAGE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 12: STORAGE GROWTH TRENDS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_STORAGE_GROWTH AS
SELECT
    usage_date,
    database_name,
    SUM(average_database_bytes) / 1024 / 1024 / 1024 AS total_gb,
    SUM(average_database_bytes) / 1024 / 1024 / 1024 / 1024 AS total_tb,
    -- Daily growth
    LAG(SUM(average_database_bytes) / 1024 / 1024 / 1024) OVER (
        PARTITION BY database_name ORDER BY usage_date
    ) AS prev_day_gb,
    SUM(average_database_bytes) / 1024 / 1024 / 1024 - 
        COALESCE(LAG(SUM(average_database_bytes) / 1024 / 1024 / 1024) OVER (
            PARTITION BY database_name ORDER BY usage_date
        ), 0) AS daily_growth_gb,
    -- Growth percentage
    ROUND((SUM(average_database_bytes) / 1024 / 1024 / 1024 - 
        COALESCE(LAG(SUM(average_database_bytes) / 1024 / 1024 / 1024) OVER (
            PARTITION BY database_name ORDER BY usage_date
        ), SUM(average_database_bytes) / 1024 / 1024 / 1024)) / 
        NULLIF(LAG(SUM(average_database_bytes) / 1024 / 1024 / 1024) OVER (
            PARTITION BY database_name ORDER BY usage_date
        ), 0) * 100, 2) AS daily_growth_pct,
    -- 7-day growth average
    AVG(SUM(average_database_bytes) / 1024 / 1024 / 1024 - 
        COALESCE(LAG(SUM(average_database_bytes) / 1024 / 1024 / 1024) OVER (
            PARTITION BY database_name ORDER BY usage_date
        ), 0)) OVER (
        PARTITION BY database_name 
        ORDER BY usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_daily_growth_7d_gb,
    -- Monthly storage cost ($23/TB/month)
    ROUND(SUM(average_database_bytes) / 1024 / 1024 / 1024 / 1024 * 23, 2) AS est_monthly_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
WHERE database_name = 'EDW'
  AND usage_date >= DATEADD('day', -90, CURRENT_DATE())
GROUP BY usage_date, database_name
ORDER BY usage_date DESC;

COMMENT ON VIEW O2C_ENH_STORAGE_GROWTH IS 
    'Database storage growth trends with cost projections';

SELECT '✅ VIEW 12 CREATED: O2C_ENH_STORAGE_GROWTH' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 13: TABLE SIZES RANKING
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TABLE_SIZES AS
SELECT
    schema_name,
    table_name,
    table_type,
    row_count,
    size_mb,
    size_gb,
    monthly_cost_usd,
    size_tier,
    rows_per_mb,
    created_at,
    last_altered,
    -- Rank by size
    RANK() OVER (ORDER BY size_mb DESC) AS size_rank,
    -- Percentage of total
    ROUND(size_mb * 100.0 / SUM(size_mb) OVER (), 2) AS pct_of_total,
    -- Cumulative percentage
    ROUND(SUM(size_mb) OVER (ORDER BY size_mb DESC ROWS UNBOUNDED PRECEDING) * 100.0 / 
        SUM(size_mb) OVER (), 2) AS cumulative_pct
FROM O2C_ENH_STORAGE_USAGE
WHERE table_type = 'BASE TABLE'
ORDER BY size_mb DESC;

COMMENT ON VIEW O2C_ENH_TABLE_SIZES IS 
    'Table sizes ranked with cumulative percentages';

SELECT '✅ VIEW 13 CREATED: O2C_ENH_TABLE_SIZES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 14: STORAGE FORECAST
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_STORAGE_FORECAST AS
WITH recent_growth AS (
    SELECT
        database_name,
        AVG(daily_growth_gb) AS avg_daily_growth_gb
    FROM O2C_ENH_STORAGE_GROWTH
    WHERE usage_date >= DATEADD('day', -30, CURRENT_DATE())
      AND daily_growth_gb IS NOT NULL
    GROUP BY database_name
),
current_storage AS (
    SELECT
        database_name,
        MAX(total_gb) AS current_gb
    FROM O2C_ENH_STORAGE_GROWTH
    WHERE usage_date >= DATEADD('day', -3, CURRENT_DATE())
    GROUP BY database_name
)
SELECT
    cs.database_name,
    ROUND(cs.current_gb, 2) AS current_gb,
    ROUND(rg.avg_daily_growth_gb, 4) AS avg_daily_growth_gb,
    -- 30-day forecast
    ROUND(cs.current_gb + (rg.avg_daily_growth_gb * 30), 2) AS forecast_30d_gb,
    ROUND((cs.current_gb + (rg.avg_daily_growth_gb * 30)) / 1024 * 23, 2) AS forecast_30d_cost_usd,
    -- 90-day forecast
    ROUND(cs.current_gb + (rg.avg_daily_growth_gb * 90), 2) AS forecast_90d_gb,
    ROUND((cs.current_gb + (rg.avg_daily_growth_gb * 90)) / 1024 * 23, 2) AS forecast_90d_cost_usd,
    -- Annual forecast
    ROUND(cs.current_gb + (rg.avg_daily_growth_gb * 365), 2) AS forecast_1yr_gb,
    ROUND((cs.current_gb + (rg.avg_daily_growth_gb * 365)) / 1024 * 23, 2) AS forecast_1yr_monthly_cost_usd,
    -- Growth rate classification
    CASE 
        WHEN rg.avg_daily_growth_gb > 1 THEN '🔴 RAPID (>1GB/day)'
        WHEN rg.avg_daily_growth_gb > 0.1 THEN '🟡 MODERATE (100MB-1GB/day)'
        WHEN rg.avg_daily_growth_gb > 0.01 THEN '🟢 SLOW (10-100MB/day)'
        ELSE '⚪ MINIMAL (<10MB/day)'
    END AS growth_status,
    CURRENT_TIMESTAMP() AS forecast_generated_at
FROM current_storage cs
LEFT JOIN recent_growth rg ON cs.database_name = rg.database_name;

COMMENT ON VIEW O2C_ENH_STORAGE_FORECAST IS 
    'Storage growth forecast with cost projections';

SELECT '✅ VIEW 14 CREATED: O2C_ENH_STORAGE_FORECAST' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 15: STORAGE ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_STORAGE AS
-- Rapid growth alert
SELECT
    'RAPID_GROWTH' AS alert_type,
    CURRENT_TIMESTAMP() AS detected_at,
    database_name AS affected_entity,
    avg_daily_growth_gb AS metric_value,
    'Storage growing at ' || ROUND(avg_daily_growth_gb, 2) || ' GB/day' AS alert_description,
    CASE 
        WHEN avg_daily_growth_gb > 5 THEN 'CRITICAL'
        WHEN avg_daily_growth_gb > 1 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    'Review data retention policies' AS recommendation
FROM O2C_ENH_STORAGE_FORECAST
WHERE avg_daily_growth_gb > 0.5

UNION ALL

-- Large table alert
SELECT
    'LARGE_TABLE',
    CURRENT_TIMESTAMP(),
    schema_name || '.' || table_name,
    size_gb,
    'Table size: ' || size_gb || ' GB (' || pct_of_total || '% of total)',
    CASE 
        WHEN size_gb > 50 THEN 'HIGH'
        ELSE 'MEDIUM'
    END,
    'Consider partitioning, archiving, or clustering'
FROM O2C_ENH_TABLE_SIZES
WHERE size_gb > 10

UNION ALL

-- Cost projection alert
SELECT
    'HIGH_COST_PROJECTION',
    CURRENT_TIMESTAMP(),
    database_name,
    forecast_1yr_monthly_cost_usd,
    '1-year storage cost projection: $' || forecast_1yr_monthly_cost_usd || '/month',
    CASE 
        WHEN forecast_1yr_monthly_cost_usd > 100 THEN 'HIGH'
        ELSE 'MEDIUM'
    END,
    'Review archival and data lifecycle policies'
FROM O2C_ENH_STORAGE_FORECAST
WHERE forecast_1yr_monthly_cost_usd > 50

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        ELSE 3 
    END;

COMMENT ON VIEW O2C_ENH_ALERT_STORAGE IS 
    'Storage growth and cost alerts';

SELECT '✅ VIEW 15 CREATED: O2C_ENH_ALERT_STORAGE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 4: TASK & STREAM MONITORING (Category 9)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 4: Task & Stream Monitoring' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 16: TASK HISTORY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TASK_HISTORY AS
SELECT
    query_id,
    name AS task_name,
    database_name,
    schema_name,
    state AS execution_status,
    scheduled_time,
    completed_time,
    DATEDIFF('second', scheduled_time, completed_time) AS execution_seconds,
    error_code,
    error_message,
    -- Time to start (schedule lag)
    DATEDIFF('second', scheduled_time, 
        COALESCE(query_start_time, completed_time)) AS schedule_lag_seconds,
    -- Status indicator
    CASE 
        WHEN state = 'SUCCEEDED' THEN '✅ SUCCESS'
        WHEN state = 'FAILED' THEN '🔴 FAILED'
        WHEN state = 'CANCELLED' THEN '🟡 CANCELLED'
        WHEN state = 'SKIPPED' THEN '⚪ SKIPPED'
        ELSE state
    END AS status_display,
    -- Performance classification
    CASE 
        WHEN DATEDIFF('second', scheduled_time, completed_time) > 600 THEN '🔴 LONG (>10 min)'
        WHEN DATEDIFF('second', scheduled_time, completed_time) > 120 THEN '🟡 MODERATE (2-10 min)'
        WHEN DATEDIFF('second', scheduled_time, completed_time) > 30 THEN '🟢 QUICK (30s-2 min)'
        ELSE '⚪ FAST (<30s)'
    END AS duration_tier
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE database_name = 'EDW'
  AND scheduled_time >= DATEADD('day', -14, CURRENT_DATE())
ORDER BY scheduled_time DESC;

COMMENT ON VIEW O2C_ENH_TASK_HISTORY IS 
    'Task execution history with performance metrics';

SELECT '✅ VIEW 16 CREATED: O2C_ENH_TASK_HISTORY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 17: TASK PERFORMANCE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TASK_PERFORMANCE AS
SELECT
    task_name,
    database_name,
    schema_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 1) AS success_rate_pct,
    ROUND(AVG(execution_seconds), 2) AS avg_execution_seconds,
    ROUND(MAX(execution_seconds), 2) AS max_execution_seconds,
    ROUND(MIN(execution_seconds), 2) AS min_execution_seconds,
    ROUND(STDDEV(execution_seconds), 2) AS stddev_seconds,
    ROUND(AVG(schedule_lag_seconds), 2) AS avg_schedule_lag_seconds,
    MIN(scheduled_time) AS first_run,
    MAX(scheduled_time) AS last_run,
    -- Health indicator
    CASE 
        WHEN SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) > 5 THEN '🔴 UNHEALTHY'
        WHEN SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN '🟡 DEGRADED'
        ELSE '🟢 HEALTHY'
    END AS health_status,
    -- Performance trend
    CASE 
        WHEN AVG(execution_seconds) > 300 THEN '🔴 SLOW'
        WHEN AVG(execution_seconds) > 60 THEN '🟡 MODERATE'
        ELSE '🟢 FAST'
    END AS performance_status
FROM O2C_ENH_TASK_HISTORY
WHERE scheduled_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY task_name, database_name, schema_name
ORDER BY failed_runs DESC, avg_execution_seconds DESC;

COMMENT ON VIEW O2C_ENH_TASK_PERFORMANCE IS 
    'Task performance metrics with health indicators';

SELECT '✅ VIEW 17 CREATED: O2C_ENH_TASK_PERFORMANCE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 18: STREAM LAG ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_STREAM_LAG AS
SELECT
    stream_catalog AS database_name,
    stream_schema AS schema_name,
    stream_name,
    table_name AS source_table,
    stream_type,
    stale,
    stale_after,
    -- Lag calculation
    CASE 
        WHEN stale = 'YES' THEN NULL
        ELSE DATEDIFF('hour', COALESCE(stale_after, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP())
    END AS hours_until_stale,
    -- Status
    CASE 
        WHEN stale = 'YES' THEN '🔴 STALE - Data may be lost'
        WHEN DATEDIFF('hour', COALESCE(stale_after, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP()) < 24 
        THEN '🟡 WARNING - Will stale soon'
        ELSE '🟢 HEALTHY'
    END AS stream_status,
    -- Recommendation
    CASE 
        WHEN stale = 'YES' THEN 'Recreate stream immediately - data has been missed'
        WHEN DATEDIFF('hour', COALESCE(stale_after, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP()) < 24 
        THEN 'Process stream soon to avoid data loss'
        ELSE 'No action needed'
    END AS recommendation
FROM EDW.INFORMATION_SCHEMA.STREAMS
WHERE stream_schema LIKE 'O2C%'
ORDER BY 
    CASE stale WHEN 'YES' THEN 1 ELSE 2 END,
    hours_until_stale ASC NULLS LAST;

COMMENT ON VIEW O2C_ENH_STREAM_LAG IS 
    'Stream staleness and lag analysis';

SELECT '✅ VIEW 18 CREATED: O2C_ENH_STREAM_LAG' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 19: TASK DEPENDENCIES (Basic)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_TASK_DEPENDENCIES AS
SELECT
    t.task_catalog AS database_name,
    t.task_schema AS schema_name,
    t.task_name,
    t.task_owner AS owner,
    t.schedule,
    t.state,
    t.warehouse,
    -- Predecessor info from definition
    CASE 
        WHEN t.definition ILIKE '%AFTER%' 
        THEN REGEXP_SUBSTR(t.definition, 'AFTER\\s+([\\w.]+)', 1, 1, 'ie', 1)
        ELSE NULL
    END AS predecessor_task,
    -- Depth estimation
    CASE 
        WHEN t.definition NOT ILIKE '%AFTER%' THEN 0
        ELSE 1  -- Simplified - would need recursive CTE for true depth
    END AS depth,
    -- Task type
    CASE 
        WHEN t.definition NOT ILIKE '%AFTER%' THEN 'ROOT'
        ELSE 'CHILD'
    END AS task_type,
    t.created AS created_at,
    t.last_altered
FROM EDW.INFORMATION_SCHEMA.TASKS t
WHERE t.task_schema LIKE 'O2C%'
ORDER BY task_type, task_name;

COMMENT ON VIEW O2C_ENH_TASK_DEPENDENCIES IS 
    'Task dependency analysis';

SELECT '✅ VIEW 19 CREATED: O2C_ENH_TASK_DEPENDENCIES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 20: TASK/STREAM ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_TASKS AS
-- Failed tasks
SELECT
    'TASK_FAILURE' AS alert_type,
    MAX(scheduled_time) AS detected_at,
    task_name AS affected_entity,
    failed_runs AS metric_value,
    'Task failed ' || failed_runs || ' times in last 7 days' AS alert_description,
    CASE 
        WHEN failed_runs > 5 THEN 'CRITICAL'
        WHEN failed_runs > 2 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    'Check task definition and dependencies' AS recommendation
FROM O2C_ENH_TASK_PERFORMANCE
WHERE failed_runs > 0
GROUP BY task_name, failed_runs

UNION ALL

-- Slow tasks
SELECT
    'SLOW_TASK',
    MAX(last_run),
    task_name,
    avg_execution_seconds,
    'Task avg execution: ' || ROUND(avg_execution_seconds / 60, 1) || ' minutes',
    CASE 
        WHEN avg_execution_seconds > 600 THEN 'HIGH'
        ELSE 'MEDIUM'
    END,
    'Optimize task SQL or increase resources'
FROM O2C_ENH_TASK_PERFORMANCE
WHERE avg_execution_seconds > 300
GROUP BY task_name, avg_execution_seconds

UNION ALL

-- Stale streams
SELECT
    'STALE_STREAM',
    CURRENT_TIMESTAMP(),
    schema_name || '.' || stream_name,
    0,
    'Stream is stale - data may have been lost',
    'CRITICAL',
    recommendation
FROM O2C_ENH_STREAM_LAG
WHERE stream_status LIKE '%STALE%'

UNION ALL

-- Stream about to stale
SELECT
    'STREAM_WARNING',
    CURRENT_TIMESTAMP(),
    schema_name || '.' || stream_name,
    hours_until_stale,
    'Stream will stale in ' || hours_until_stale || ' hours',
    'HIGH',
    recommendation
FROM O2C_ENH_STREAM_LAG
WHERE stream_status LIKE '%WARNING%'

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        ELSE 3 
    END,
    detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_TASKS IS 
    'Task and stream alerts';

SELECT '✅ VIEW 20 CREATED: O2C_ENH_ALERT_TASKS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 5: CONCURRENCY & CONTENTION (Category 10)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 5: Concurrency & Contention Monitoring' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 21: QUERY CONCURRENCY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_QUERY_CONCURRENCY AS
WITH minute_stats AS (
    SELECT
        DATE_TRUNC('minute', start_time) AS minute_bucket,
        warehouse_name,
        COUNT(*) AS concurrent_queries,
        SUM(CASE WHEN queued_overload_time > 0 THEN 1 ELSE 0 END) AS queued_queries,
        SUM(CASE WHEN queued_provisioning_time > 0 THEN 1 ELSE 0 END) AS provisioning_waits,
        AVG(total_elapsed_time) / 1000 AS avg_elapsed_seconds,
        MAX(total_elapsed_time) / 1000 AS max_elapsed_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND warehouse_name IS NOT NULL
      AND start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY minute_bucket, warehouse_name
)
SELECT
    DATE(minute_bucket) AS date,
    HOUR(minute_bucket) AS hour,
    warehouse_name,
    MAX(concurrent_queries) AS peak_concurrency,
    ROUND(AVG(concurrent_queries), 1) AS avg_concurrency,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY concurrent_queries), 0) AS p95_concurrency,
    SUM(queued_queries) AS total_queued_minutes,
    SUM(provisioning_waits) AS provisioning_wait_minutes,
    ROUND(AVG(avg_elapsed_seconds), 2) AS avg_query_seconds,
    -- Concurrency health
    CASE 
        WHEN MAX(concurrent_queries) > 100 THEN '🔴 VERY HIGH'
        WHEN MAX(concurrent_queries) > 50 THEN '🟠 HIGH'
        WHEN MAX(concurrent_queries) > 20 THEN '🟡 MODERATE'
        ELSE '🟢 NORMAL'
    END AS concurrency_status,
    -- Resource pressure indicator
    CASE 
        WHEN SUM(queued_queries) > 10 THEN '⚠️ Queue pressure detected'
        WHEN SUM(provisioning_waits) > 5 THEN '⚠️ Provisioning delays'
        ELSE '✅ No pressure'
    END AS resource_pressure
FROM minute_stats
GROUP BY date, hour, warehouse_name
ORDER BY date DESC, hour DESC, peak_concurrency DESC;

COMMENT ON VIEW O2C_ENH_QUERY_CONCURRENCY IS 
    'Query concurrency analysis with peak and pressure metrics';

SELECT '✅ VIEW 21 CREATED: O2C_ENH_QUERY_CONCURRENCY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 22: BLOCKED/WAITING QUERIES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_BLOCKED_QUERIES AS
SELECT
    query_id,
    start_time,
    end_time,
    user_name,
    warehouse_name,
    warehouse_size,
    ROUND(total_elapsed_time / 1000, 2) AS total_seconds,
    ROUND(queued_overload_time / 1000, 2) AS queue_overload_seconds,
    ROUND(queued_provisioning_time / 1000, 2) AS queue_provision_seconds,
    ROUND(queued_repair_time / 1000, 2) AS queue_repair_seconds,
    -- Total wait time
    ROUND((queued_overload_time + queued_provisioning_time + queued_repair_time) / 1000, 2) AS total_wait_seconds,
    -- Wait percentage
    ROUND((queued_overload_time + queued_provisioning_time + queued_repair_time) * 100.0 / 
        NULLIF(total_elapsed_time, 0), 1) AS wait_pct_of_total,
    -- Block type
    CASE 
        WHEN queued_overload_time > 0 THEN 'CAPACITY OVERLOAD'
        WHEN queued_provisioning_time > 0 THEN 'PROVISIONING'
        WHEN queued_repair_time > 0 THEN 'REPAIR'
        ELSE 'UNKNOWN'
    END AS wait_type,
    query_type,
    LEFT(query_text, 300) AS query_preview,
    -- Severity
    CASE 
        WHEN (queued_overload_time + queued_provisioning_time) / 1000 > 60 THEN '🔴 CRITICAL (>1 min wait)'
        WHEN (queued_overload_time + queued_provisioning_time) / 1000 > 30 THEN '🟠 HIGH (30-60s wait)'
        WHEN (queued_overload_time + queued_provisioning_time) / 1000 > 10 THEN '🟡 MEDIUM (10-30s wait)'
        ELSE '🟢 LOW (<10s wait)'
    END AS severity
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND (queued_overload_time > 5000 OR queued_provisioning_time > 5000 OR queued_repair_time > 5000)
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY total_wait_seconds DESC
LIMIT 100;

COMMENT ON VIEW O2C_ENH_BLOCKED_QUERIES IS 
    'Queries with significant wait/block time';

SELECT '✅ VIEW 22 CREATED: O2C_ENH_BLOCKED_QUERIES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 23: LOCK WAIT ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_LOCK_WAITS AS
SELECT
    start_time,
    query_id,
    user_name,
    warehouse_name,
    query_type,
    -- Lock wait time estimation from transaction control time
    ROUND(transaction_blocked_time / 1000, 2) AS blocked_seconds,
    ROUND(execution_time / 1000, 2) AS execution_seconds,
    ROUND(total_elapsed_time / 1000, 2) AS total_seconds,
    -- Lock impact
    ROUND(transaction_blocked_time * 100.0 / NULLIF(total_elapsed_time, 0), 1) AS lock_wait_pct,
    -- Affected object (attempt to extract)
    COALESCE(
        REGEXP_SUBSTR(query_text, 'UPDATE\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'DELETE\\s+FROM\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'MERGE\\s+INTO\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'INSERT\\s+INTO\\s+([\\w.]+)', 1, 1, 'ie', 1),
        'Unknown'
    ) AS affected_table,
    LEFT(query_text, 300) AS query_preview,
    -- Severity
    CASE 
        WHEN transaction_blocked_time / 1000 > 60 THEN '🔴 CRITICAL LOCK'
        WHEN transaction_blocked_time / 1000 > 30 THEN '🟠 HIGH'
        WHEN transaction_blocked_time / 1000 > 10 THEN '🟡 MEDIUM'
        ELSE '🟢 LOW'
    END AS severity
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND transaction_blocked_time > 5000  -- More than 5 seconds blocked
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY transaction_blocked_time DESC
LIMIT 50;

COMMENT ON VIEW O2C_ENH_LOCK_WAITS IS 
    'Lock wait analysis for write operations';

SELECT '✅ VIEW 23 CREATED: O2C_ENH_LOCK_WAITS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 24: DISK SPILL ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_SPILL_ANALYSIS AS
SELECT
    DATE(start_time) AS query_date,
    warehouse_name,
    warehouse_size,
    COUNT(*) AS total_queries,
    SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) AS local_spill_queries,
    SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) AS remote_spill_queries,
    ROUND(SUM(bytes_spilled_to_local_storage) / 1024 / 1024 / 1024, 2) AS local_spill_gb,
    ROUND(SUM(bytes_spilled_to_remote_storage) / 1024 / 1024 / 1024, 2) AS remote_spill_gb,
    -- Spill rate
    ROUND(SUM(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 
        THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS spill_rate_pct,
    -- Performance impact
    CASE 
        WHEN SUM(bytes_spilled_to_remote_storage) / 1024 / 1024 / 1024 > 10 THEN '🔴 CRITICAL - Remote spill >10GB'
        WHEN SUM(bytes_spilled_to_local_storage) / 1024 / 1024 / 1024 > 50 THEN '🟠 HIGH - Local spill >50GB'
        WHEN SUM(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 
            THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) > 20 THEN '🟡 MEDIUM - >20% queries spilling'
        ELSE '🟢 HEALTHY'
    END AS spill_status,
    -- Recommendation
    CASE 
        WHEN SUM(bytes_spilled_to_remote_storage) > 0 
        THEN 'Increase warehouse size to avoid remote spill'
        WHEN SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(COUNT(*), 0) > 30 
        THEN 'Consider larger warehouse or query optimization'
        ELSE 'No action needed'
    END AS recommendation
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND warehouse_name IS NOT NULL
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY query_date, warehouse_name, warehouse_size
ORDER BY query_date DESC, remote_spill_gb DESC, local_spill_gb DESC;

COMMENT ON VIEW O2C_ENH_SPILL_ANALYSIS IS 
    'Disk spill analysis for memory pressure detection';

SELECT '✅ VIEW 24 CREATED: O2C_ENH_SPILL_ANALYSIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 25: CONTENTION ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_CONTENTION AS
-- High concurrency alerts
SELECT
    'HIGH_CONCURRENCY' AS alert_type,
    DATE_TRUNC('hour', CURRENT_TIMESTAMP()) AS detected_at,
    warehouse_name AS affected_entity,
    peak_concurrency AS metric_value,
    'Peak concurrency: ' || peak_concurrency || ' queries (P95: ' || p95_concurrency || ')' AS alert_description,
    CASE 
        WHEN peak_concurrency > 100 THEN 'CRITICAL'
        WHEN peak_concurrency > 50 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    'Consider multi-cluster or query scheduling' AS recommendation
FROM O2C_ENH_QUERY_CONCURRENCY
WHERE date >= CURRENT_DATE() - 1
  AND peak_concurrency > 40
GROUP BY warehouse_name, peak_concurrency, p95_concurrency

UNION ALL

-- Significant wait times
SELECT
    'LONG_WAIT_TIME',
    start_time,
    query_id,
    total_wait_seconds,
    'Query waited ' || total_wait_seconds || 's (' || wait_pct_of_total || '% of total time)',
    severity,
    'Review warehouse capacity and query scheduling'
FROM O2C_ENH_BLOCKED_QUERIES
WHERE total_wait_seconds > 30
  AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())

UNION ALL

-- Lock contention
SELECT
    'LOCK_CONTENTION',
    start_time,
    affected_table,
    blocked_seconds,
    'Lock wait: ' || blocked_seconds || 's on ' || affected_table,
    severity,
    'Review concurrent write patterns'
FROM O2C_ENH_LOCK_WAITS
WHERE blocked_seconds > 30
  AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())

UNION ALL

-- Remote disk spill
SELECT
    'REMOTE_SPILL',
    CURRENT_TIMESTAMP(),
    warehouse_name,
    remote_spill_gb,
    'Remote disk spill: ' || remote_spill_gb || ' GB - significant performance impact',
    'CRITICAL',
    recommendation
FROM O2C_ENH_SPILL_ANALYSIS
WHERE query_date >= CURRENT_DATE() - 1
  AND remote_spill_gb > 1

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_CONTENTION IS 
    'Concurrency and contention alerts';

SELECT '✅ VIEW 25 CREATED: O2C_ENH_ALERT_CONTENTION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;

SELECT '✅ PERMISSIONS GRANTED' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ INFRASTRUCTURE, SECURITY & OPERATIONAL MONITORING COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- List all views created in this script
SELECT 
    TABLE_NAME AS view_name,
    COMMENT AS description
FROM EDW.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%WAREHOUSE%' 
       OR TABLE_NAME LIKE '%LOGIN%'
       OR TABLE_NAME LIKE '%ACCESS%'
       OR TABLE_NAME LIKE '%ROLE%'
       OR TABLE_NAME LIKE '%STORAGE%'
       OR TABLE_NAME LIKE '%TASK%'
       OR TABLE_NAME LIKE '%STREAM%'
       OR TABLE_NAME LIKE '%CONCURRENCY%'
       OR TABLE_NAME LIKE '%BLOCKED%'
       OR TABLE_NAME LIKE '%LOCK%'
       OR TABLE_NAME LIKE '%SPILL%'
       OR TABLE_NAME LIKE '%SECURITY%'
       OR TABLE_NAME LIKE '%CONTENTION%')
ORDER BY TABLE_NAME;

