-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - SCHEMA DRIFT, DBT OBSERVABILITY & DATA INTEGRITY MONITORING
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Schema change detection, dbt-specific metrics, and data integrity checks
-- 
-- Views Created (15 total):
--   SCHEMA DRIFT DETECTION (Category 3):
--     1. O2C_ENH_SCHEMA_CURRENT_STATE     - Current schema snapshot
--     2. O2C_ENH_DDL_CHANGES              - DDL change history
--     3. O2C_ENH_COLUMN_CHANGES           - Column-level change detection
--     4. O2C_ENH_ALERT_SCHEMA_DRIFT       - Schema drift alerts
--
--   DBT-SPECIFIC OBSERVABILITY (Category 7):
--     5. O2C_ENH_DBT_TEST_COVERAGE        - Test coverage by model
--     6. O2C_ENH_DBT_MODEL_DEPENDENCIES   - Model dependency analysis
--     7. O2C_ENH_DBT_RUN_HISTORY          - dbt run history analysis
--     8. O2C_ENH_DBT_ORPHAN_MODELS        - Models not recently used
--     9. O2C_ENH_ALERT_DBT_COVERAGE       - Test coverage alerts
--
--   DATA INTEGRITY MONITORING (Category 8):
--    10. O2C_ENH_PK_VALIDATION            - Primary key uniqueness
--    11. O2C_ENH_FK_VALIDATION            - Foreign key validation
--    12. O2C_ENH_DUPLICATE_DETECTION      - Duplicate record detection
--    13. O2C_ENH_NULL_TREND_ANALYSIS      - Null rate trends over time
--    14. O2C_ENH_DATA_CONSISTENCY         - Cross-table consistency
--    15. O2C_ENH_ALERT_DATA_INTEGRITY     - Data integrity alerts
--
-- Prerequisites:
--   - O2C_ENHANCED_MONITORING_SETUP.sql executed
--   - O2C_ENHANCED_AUDIT_SETUP.sql executed
--   - dbt_o2c_enhanced project has run at least once
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_ENHANCED_MONITORING;

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '🚀 STARTING: Schema Drift, dbt & Data Integrity Monitoring Setup' AS status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 1: SCHEMA DRIFT DETECTION (Category 3)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 1: Schema Drift Detection' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 1: CURRENT SCHEMA STATE SNAPSHOT
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_SCHEMA_CURRENT_STATE AS
SELECT 
    c.TABLE_CATALOG AS database_name,
    c.TABLE_SCHEMA AS schema_name,
    c.TABLE_NAME AS table_name,
    t.TABLE_TYPE AS object_type,
    c.COLUMN_NAME,
    c.ORDINAL_POSITION AS column_position,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH AS max_length,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT,
    c.COMMENT AS column_comment,
    t.ROW_COUNT AS table_row_count,
    t.BYTES AS table_bytes,
    ROUND(t.BYTES / 1024 / 1024, 2) AS table_size_mb,
    t.CREATED AS table_created,
    t.LAST_ALTERED AS table_last_altered,
    -- Generate column signature for change detection
    MD5(c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '.' || c.COLUMN_NAME || '|' || 
        c.DATA_TYPE || '|' || COALESCE(c.CHARACTER_MAXIMUM_LENGTH::VARCHAR, '') || '|' ||
        COALESCE(c.NUMERIC_PRECISION::VARCHAR, '') || '|' || c.IS_NULLABLE) AS column_signature,
    CURRENT_TIMESTAMP() AS snapshot_time
FROM EDW.INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN EDW.INFORMATION_SCHEMA.TABLES t 
    ON c.TABLE_CATALOG = t.TABLE_CATALOG 
    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA 
    AND c.TABLE_NAME = t.TABLE_NAME
WHERE c.TABLE_SCHEMA LIKE 'O2C_ENHANCED%'
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;

COMMENT ON VIEW O2C_ENH_SCHEMA_CURRENT_STATE IS 
    'Current schema snapshot for O2C Enhanced tables with column signatures';

SELECT '✅ VIEW 1 CREATED: O2C_ENH_SCHEMA_CURRENT_STATE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 2: DDL CHANGE HISTORY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DDL_CHANGES AS
SELECT
    query_id,
    start_time AS change_time,
    user_name,
    role_name,
    warehouse_name,
    query_type,
    -- Extract DDL operation type
    CASE 
        WHEN query_type LIKE 'CREATE%' THEN 'CREATE'
        WHEN query_type LIKE 'ALTER%' THEN 'ALTER'
        WHEN query_type LIKE 'DROP%' THEN 'DROP'
        WHEN query_type LIKE 'RENAME%' THEN 'RENAME'
        ELSE 'OTHER'
    END AS ddl_operation,
    -- Extract object type
    CASE 
        WHEN query_type LIKE '%TABLE%' THEN 'TABLE'
        WHEN query_type LIKE '%VIEW%' THEN 'VIEW'
        WHEN query_type LIKE '%SCHEMA%' THEN 'SCHEMA'
        WHEN query_type LIKE '%PROCEDURE%' THEN 'PROCEDURE'
        WHEN query_type LIKE '%FUNCTION%' THEN 'FUNCTION'
        ELSE 'OTHER'
    END AS object_type,
    -- Extract object name (best effort regex)
    COALESCE(
        REGEXP_SUBSTR(query_text, '(?:TABLE|VIEW|SCHEMA)\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, '(?:CREATE|ALTER|DROP)\\s+(?:OR\\s+REPLACE\\s+)?(?:TABLE|VIEW)\\s+([\\w.]+)', 1, 1, 'ie', 1),
        'UNKNOWN'
    ) AS affected_object,
    execution_status,
    error_code,
    error_message,
    LEFT(query_text, 1000) AS query_text_preview,
    -- Impact assessment
    CASE 
        WHEN query_type LIKE 'DROP%' THEN '🔴 HIGH - Object Dropped'
        WHEN query_type LIKE 'ALTER%' AND query_text ILIKE '%DROP%COLUMN%' THEN '🔴 HIGH - Column Dropped'
        WHEN query_type LIKE 'ALTER%' AND query_text ILIKE '%MODIFY%' THEN '🟠 MEDIUM - Column Modified'
        WHEN query_type LIKE 'ALTER%' AND query_text ILIKE '%ADD%COLUMN%' THEN '🟡 LOW - Column Added'
        WHEN query_type LIKE 'CREATE%' THEN '🟢 INFO - Object Created'
        ELSE '⚪ INFO'
    END AS impact_level,
    -- Severity for alerting
    CASE 
        WHEN query_type LIKE 'DROP%' THEN 'CRITICAL'
        WHEN query_type LIKE 'ALTER%' AND query_text ILIKE '%DROP%COLUMN%' THEN 'HIGH'
        WHEN query_type LIKE 'ALTER%' AND query_text ILIKE '%MODIFY%TYPE%' THEN 'HIGH'
        WHEN query_type LIKE 'ALTER%' THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND query_type IN ('CREATE_TABLE', 'CREATE_TABLE_AS_SELECT', 'ALTER_TABLE', 'DROP_TABLE',
                     'CREATE_VIEW', 'ALTER_VIEW', 'DROP_VIEW',
                     'CREATE_SCHEMA', 'ALTER_SCHEMA', 'DROP_SCHEMA',
                     'RENAME_TABLE', 'RENAME_COLUMN')
  AND (schema_name LIKE 'O2C_ENHANCED%' OR query_text ILIKE '%O2C_ENHANCED%')
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY change_time DESC;

COMMENT ON VIEW O2C_ENH_DDL_CHANGES IS 
    'DDL change history for O2C Enhanced objects with impact assessment';

SELECT '✅ VIEW 2 CREATED: O2C_ENH_DDL_CHANGES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 3: COLUMN CHANGES TRACKING (Based on table alterations)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_COLUMN_CHANGES AS
WITH table_history AS (
    SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        LAST_ALTERED,
        ROW_COUNT,
        BYTES
    FROM EDW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA LIKE 'O2C_ENHANCED%'
)
SELECT
    th.TABLE_SCHEMA AS schema_name,
    th.TABLE_NAME AS table_name,
    th.LAST_ALTERED AS last_schema_change,
    th.ROW_COUNT AS current_row_count,
    ROUND(th.BYTES / 1024 / 1024, 2) AS current_size_mb,
    -- Count columns
    (SELECT COUNT(*) 
     FROM EDW.INFORMATION_SCHEMA.COLUMNS c 
     WHERE c.TABLE_SCHEMA = th.TABLE_SCHEMA 
       AND c.TABLE_NAME = th.TABLE_NAME) AS column_count,
    -- Recent alterations indicator
    CASE 
        WHEN th.LAST_ALTERED >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) 
        THEN '🔴 CHANGED (Last 24h)'
        WHEN th.LAST_ALTERED >= DATEADD('day', -7, CURRENT_TIMESTAMP()) 
        THEN '🟡 CHANGED (Last 7d)'
        WHEN th.LAST_ALTERED >= DATEADD('day', -30, CURRENT_TIMESTAMP()) 
        THEN '🟢 CHANGED (Last 30d)'
        ELSE '⚪ STABLE'
    END AS change_status,
    DATEDIFF('day', th.LAST_ALTERED, CURRENT_TIMESTAMP()) AS days_since_change
FROM table_history th
ORDER BY th.LAST_ALTERED DESC;

COMMENT ON VIEW O2C_ENH_COLUMN_CHANGES IS 
    'Table-level change tracking based on LAST_ALTERED timestamp';

SELECT '✅ VIEW 3 CREATED: O2C_ENH_COLUMN_CHANGES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 4: SCHEMA DRIFT ALERT VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_SCHEMA_DRIFT AS
-- Recent DDL changes
SELECT
    'DDL_CHANGE' AS alert_type,
    change_time AS detected_at,
    affected_object,
    ddl_operation || ' ' || object_type AS change_description,
    severity,
    impact_level,
    user_name AS changed_by,
    query_text_preview AS details
FROM O2C_ENH_DDL_CHANGES
WHERE change_time >= DATEADD('day', -7, CURRENT_DATE())
  AND severity IN ('CRITICAL', 'HIGH', 'MEDIUM')

UNION ALL

-- Tables changed in last 24 hours
SELECT
    'TABLE_ALTERED' AS alert_type,
    last_schema_change AS detected_at,
    schema_name || '.' || table_name AS affected_object,
    'Table schema was modified' AS change_description,
    CASE 
        WHEN days_since_change = 0 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    change_status AS impact_level,
    NULL AS changed_by,
    'Columns: ' || column_count || ', Rows: ' || current_row_count AS details
FROM O2C_ENH_COLUMN_CHANGES
WHERE days_since_change <= 7
  AND change_status != '⚪ STABLE'

ORDER BY detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_SCHEMA_DRIFT IS 
    'Schema drift alerts from DDL changes and table alterations';

SELECT '✅ VIEW 4 CREATED: O2C_ENH_ALERT_SCHEMA_DRIFT' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 2: DBT-SPECIFIC OBSERVABILITY (Category 7)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 2: dbt-Specific Observability' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 5: DBT TEST COVERAGE ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DBT_TEST_COVERAGE AS
WITH models AS (
    -- Get distinct models from model executions
    SELECT DISTINCT 
        model_name,
        schema_name,
        MAX(run_started_at) AS last_run
    FROM O2C_ENH_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
      AND status = 'SUCCESS'
    GROUP BY model_name, schema_name
),
test_executions AS (
    -- Get test execution counts per model
    SELECT
        REGEXP_SUBSTR(query_text, 'FROM\\s+(?:EDW\\.)?([\\w.]+)', 1, 1, 'ie', 1) AS tested_table,
        COUNT(*) AS test_count,
        SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS passed_tests,
        SUM(CASE WHEN execution_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_tests,
        MAX(start_time) AS last_test_run
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND (query_text ILIKE '%dbt_test%'
           OR query_text ILIKE '%test_%'
           OR (query_text ILIKE '%count(*)%' AND query_text ILIKE '%where%not%'))
      AND query_type = 'SELECT'
      AND start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY tested_table
)
SELECT 
    m.model_name,
    m.schema_name,
    m.last_run AS last_model_run,
    COALESCE(t.test_count, 0) AS test_count,
    COALESCE(t.passed_tests, 0) AS passed_tests,
    COALESCE(t.failed_tests, 0) AS failed_tests,
    t.last_test_run,
    -- Coverage status
    CASE 
        WHEN t.test_count IS NULL OR t.test_count = 0 THEN '❌ NO TESTS'
        WHEN t.test_count < 2 THEN '🟡 LOW COVERAGE (1 test)'
        WHEN t.test_count < 5 THEN '🟢 MODERATE COVERAGE (2-4 tests)'
        ELSE '✅ GOOD COVERAGE (5+ tests)'
    END AS coverage_status,
    -- Test health
    CASE 
        WHEN t.test_count IS NULL OR t.test_count = 0 THEN 'N/A'
        WHEN t.failed_tests > 0 THEN '🔴 FAILING'
        ELSE '🟢 PASSING'
    END AS test_health,
    -- Pass rate
    CASE 
        WHEN COALESCE(t.test_count, 0) > 0 
        THEN ROUND(t.passed_tests * 100.0 / t.test_count, 1)
        ELSE NULL
    END AS pass_rate_pct
FROM models m
LEFT JOIN test_executions t 
    ON UPPER(t.tested_table) LIKE '%' || UPPER(m.model_name) || '%'
ORDER BY 
    CASE coverage_status
        WHEN '❌ NO TESTS' THEN 1
        WHEN '🟡 LOW COVERAGE (1 test)' THEN 2
        WHEN '🟢 MODERATE COVERAGE (2-4 tests)' THEN 3
        ELSE 4
    END,
    m.model_name;

COMMENT ON VIEW O2C_ENH_DBT_TEST_COVERAGE IS 
    'dbt test coverage analysis by model with pass rate';

SELECT '✅ VIEW 5 CREATED: O2C_ENH_DBT_TEST_COVERAGE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 6: DBT MODEL DEPENDENCIES (Inferred from Query Patterns)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DBT_MODEL_DEPENDENCIES AS
WITH model_queries AS (
    SELECT DISTINCT
        -- Target model (what's being created/modified)
        COALESCE(
            REGEXP_SUBSTR(query_text, 'CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:TABLE|VIEW)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:EDW\\.)?([\\w.]+)', 1, 1, 'ie', 1),
            REGEXP_SUBSTR(query_text, 'INSERT\\s+(?:INTO\\s+)?(?:EDW\\.)?([\\w.]+)', 1, 1, 'ie', 1),
            REGEXP_SUBSTR(query_text, 'MERGE\\s+INTO\\s+(?:EDW\\.)?([\\w.]+)', 1, 1, 'ie', 1)
        ) AS target_model,
        -- Source models (what's being read from)
        query_text,
        start_time
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND schema_name LIKE 'O2C_ENHANCED%'
      AND (query_text ILIKE '%create%or%replace%'
           OR query_text ILIKE '%insert%into%'
           OR query_text ILIKE '%merge%into%')
      AND start_time >= DATEADD('day', -7, CURRENT_DATE())
      AND execution_status = 'SUCCESS'
),
dependencies AS (
    SELECT DISTINCT
        SPLIT_PART(target_model, '.', -1) AS model_name,
        -- Extract source tables from FROM/JOIN clauses
        REGEXP_SUBSTR(query_text, 'FROM\\s+(?:EDW\\.)?([\\w_]+\\.\\w+)', 1, 1, 'ie', 1) AS source_1,
        REGEXP_SUBSTR(query_text, 'JOIN\\s+(?:EDW\\.)?([\\w_]+\\.\\w+)', 1, 1, 'ie', 1) AS source_2,
        REGEXP_SUBSTR(query_text, 'JOIN\\s+(?:EDW\\.)?([\\w_]+\\.\\w+)', 1, 2, 'ie', 1) AS source_3
    FROM model_queries
    WHERE target_model IS NOT NULL
)
SELECT 
    model_name,
    LISTAGG(DISTINCT COALESCE(source_1, ''), ', ') AS primary_sources,
    COUNT(DISTINCT source_1) AS source_count,
    CASE 
        WHEN COUNT(DISTINCT source_1) > 5 THEN '🔴 HIGH COMPLEXITY (>5 sources)'
        WHEN COUNT(DISTINCT source_1) > 3 THEN '🟡 MODERATE COMPLEXITY (3-5 sources)'
        WHEN COUNT(DISTINCT source_1) > 1 THEN '🟢 LOW COMPLEXITY (1-2 sources)'
        ELSE '⚪ SIMPLE (1 source)'
    END AS complexity_indicator
FROM dependencies
WHERE model_name IS NOT NULL
GROUP BY model_name
ORDER BY source_count DESC;

COMMENT ON VIEW O2C_ENH_DBT_MODEL_DEPENDENCIES IS 
    'dbt model dependency analysis inferred from query patterns';

SELECT '✅ VIEW 6 CREATED: O2C_ENH_DBT_MODEL_DEPENDENCIES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 7: DBT RUN HISTORY ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DBT_RUN_HISTORY AS
SELECT
    r.run_id,
    r.project_name,
    r.environment,
    r.run_started_at,
    r.run_ended_at,
    r.run_duration_seconds,
    r.run_status,
    r.run_command,
    r.warehouse_name,
    r.user_name,
    r.models_run,
    r.models_success,
    r.models_failed,
    r.models_skipped,
    -- Success rate
    ROUND(r.models_success * 100.0 / NULLIF(r.models_run, 0), 1) AS success_rate_pct,
    -- Duration category
    CASE 
        WHEN r.run_duration_seconds > 600 THEN '🔴 LONG (>10 min)'
        WHEN r.run_duration_seconds > 300 THEN '🟡 MODERATE (5-10 min)'
        WHEN r.run_duration_seconds > 60 THEN '🟢 QUICK (1-5 min)'
        ELSE '⚪ FAST (<1 min)'
    END AS duration_category,
    -- Run health
    CASE 
        WHEN r.run_status = 'FAILED' THEN '🔴 FAILED'
        WHEN r.models_failed > 0 THEN '🟠 PARTIAL FAILURE'
        WHEN r.run_status = 'SUCCESS' THEN '🟢 SUCCESS'
        ELSE '⚪ ' || r.run_status
    END AS run_health,
    -- Comparison with average
    ROUND(r.run_duration_seconds - AVG(r.run_duration_seconds) OVER (), 0) AS variance_from_avg_seconds
FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
WHERE r.run_started_at >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY r.run_started_at DESC;

COMMENT ON VIEW O2C_ENH_DBT_RUN_HISTORY IS 
    'dbt run history with performance and health analysis';

SELECT '✅ VIEW 7 CREATED: O2C_ENH_DBT_RUN_HISTORY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 8: ORPHAN MODELS (Models not recently executed)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DBT_ORPHAN_MODELS AS
WITH model_activity AS (
    SELECT 
        model_name,
        schema_name,
        MAX(run_started_at) AS last_execution,
        COUNT(*) AS total_executions_30d,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_executions
    FROM O2C_ENH_MODEL_EXECUTIONS
    WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY model_name, schema_name
),
all_tables AS (
    SELECT 
        TABLE_SCHEMA AS schema_name,
        TABLE_NAME AS table_name,
        CREATED AS created_at,
        LAST_ALTERED AS last_altered
    FROM EDW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA LIKE 'O2C_ENHANCED%'
      AND TABLE_TYPE = 'BASE TABLE'
)
SELECT 
    t.schema_name,
    t.table_name,
    t.created_at,
    t.last_altered,
    COALESCE(m.last_execution, t.last_altered) AS last_activity,
    COALESCE(m.total_executions_30d, 0) AS executions_30d,
    DATEDIFF('day', COALESCE(m.last_execution, t.last_altered), CURRENT_TIMESTAMP()) AS days_inactive,
    CASE 
        WHEN m.last_execution IS NULL AND DATEDIFF('day', t.last_altered, CURRENT_TIMESTAMP()) > 30 
        THEN '🔴 ORPHAN (>30 days, never run)'
        WHEN DATEDIFF('day', COALESCE(m.last_execution, t.last_altered), CURRENT_TIMESTAMP()) > 30 
        THEN '🟠 STALE (>30 days inactive)'
        WHEN DATEDIFF('day', COALESCE(m.last_execution, t.last_altered), CURRENT_TIMESTAMP()) > 14 
        THEN '🟡 AGING (>14 days inactive)'
        WHEN DATEDIFF('day', COALESCE(m.last_execution, t.last_altered), CURRENT_TIMESTAMP()) > 7 
        THEN '🟢 MODERATE (>7 days inactive)'
        ELSE '⚪ ACTIVE'
    END AS activity_status,
    CASE 
        WHEN m.last_execution IS NULL THEN 'Consider removing if unused'
        WHEN DATEDIFF('day', m.last_execution, CURRENT_TIMESTAMP()) > 30 THEN 'Review necessity'
        ELSE 'No action needed'
    END AS recommendation
FROM all_tables t
LEFT JOIN model_activity m 
    ON UPPER(t.table_name) = UPPER(m.model_name)
ORDER BY days_inactive DESC;

COMMENT ON VIEW O2C_ENH_DBT_ORPHAN_MODELS IS 
    'Identifies models/tables not recently executed (potential orphans)';

SELECT '✅ VIEW 8 CREATED: O2C_ENH_DBT_ORPHAN_MODELS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 9: DBT COVERAGE ALERT VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_DBT_COVERAGE AS
-- Models without tests
SELECT
    'NO_TESTS' AS alert_type,
    model_name,
    schema_name,
    coverage_status,
    'MEDIUM' AS severity,
    'Model has no associated tests' AS alert_description,
    CURRENT_TIMESTAMP() AS detected_at
FROM O2C_ENH_DBT_TEST_COVERAGE
WHERE coverage_status = '❌ NO TESTS'

UNION ALL

-- Models with failing tests
SELECT
    'FAILING_TESTS' AS alert_type,
    model_name,
    schema_name,
    test_health,
    'HIGH' AS severity,
    'Model has ' || failed_tests || ' failing tests out of ' || test_count AS alert_description,
    last_test_run AS detected_at
FROM O2C_ENH_DBT_TEST_COVERAGE
WHERE test_health = '🔴 FAILING'

UNION ALL

-- Orphan models
SELECT
    'ORPHAN_MODEL' AS alert_type,
    table_name AS model_name,
    schema_name,
    activity_status,
    CASE 
        WHEN activity_status LIKE '%ORPHAN%' THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    'Table inactive for ' || days_inactive || ' days. ' || recommendation AS alert_description,
    last_activity AS detected_at
FROM O2C_ENH_DBT_ORPHAN_MODELS
WHERE activity_status IN ('🔴 ORPHAN (>30 days, never run)', '🟠 STALE (>30 days inactive)')

ORDER BY 
    CASE severity
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        ELSE 3
    END,
    detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_DBT_COVERAGE IS 
    'dbt coverage and health alerts';

SELECT '✅ VIEW 9 CREATED: O2C_ENH_ALERT_DBT_COVERAGE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ███████████████████████████████████████████████████████████████████████████████
-- SECTION 3: DATA INTEGRITY MONITORING (Category 8)
-- ███████████████████████████████████████████████████████████████████████████████
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '📋 SECTION 3: Data Integrity Monitoring' AS section;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 10: PRIMARY KEY VALIDATION
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_PK_VALIDATION AS
-- Dimension: Customer
SELECT 
    'DIM_O2C_CUSTOMER' AS table_name,
    'customer_key' AS pk_column,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_key) AS unique_keys,
    COUNT(*) - COUNT(DISTINCT customer_key) AS duplicate_count,
    COUNT(CASE WHEN customer_key IS NULL THEN 1 END) AS null_pk_count,
    ROUND(COUNT(DISTINCT customer_key) * 100.0 / NULLIF(COUNT(*), 0), 2) AS uniqueness_pct,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT customer_key) AND COUNT(CASE WHEN customer_key IS NULL THEN 1 END) = 0
        THEN '✅ VALID'
        WHEN COUNT(CASE WHEN customer_key IS NULL THEN 1 END) > 0 THEN '🔴 NULL PKs FOUND'
        ELSE '🔴 DUPLICATES FOUND'
    END AS pk_status,
    CURRENT_TIMESTAMP() AS validated_at
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER

UNION ALL

-- Core: Reconciliation
SELECT 
    'DM_O2C_RECONCILIATION',
    'reconciliation_key',
    COUNT(*),
    COUNT(DISTINCT reconciliation_key),
    COUNT(*) - COUNT(DISTINCT reconciliation_key),
    COUNT(CASE WHEN reconciliation_key IS NULL THEN 1 END),
    ROUND(COUNT(DISTINCT reconciliation_key) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT reconciliation_key) AND COUNT(CASE WHEN reconciliation_key IS NULL THEN 1 END) = 0
        THEN '✅ VALID'
        WHEN COUNT(CASE WHEN reconciliation_key IS NULL THEN 1 END) > 0 THEN '🔴 NULL PKs FOUND'
        ELSE '🔴 DUPLICATES FOUND'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL

-- Events
SELECT 
    'FACT_O2C_EVENTS',
    'event_key',
    COUNT(*),
    COUNT(DISTINCT event_key),
    COUNT(*) - COUNT(DISTINCT event_key),
    COUNT(CASE WHEN event_key IS NULL THEN 1 END),
    ROUND(COUNT(DISTINCT event_key) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT event_key) AND COUNT(CASE WHEN event_key IS NULL THEN 1 END) = 0
        THEN '✅ VALID'
        WHEN COUNT(CASE WHEN event_key IS NULL THEN 1 END) > 0 THEN '🔴 NULL PKs FOUND'
        ELSE '🔴 DUPLICATES FOUND'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS

UNION ALL

-- Aggregates
SELECT 
    'AGG_O2C_BY_CUSTOMER',
    'customer_key',
    COUNT(*),
    COUNT(DISTINCT customer_key),
    COUNT(*) - COUNT(DISTINCT customer_key),
    COUNT(CASE WHEN customer_key IS NULL THEN 1 END),
    ROUND(COUNT(DISTINCT customer_key) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT customer_key) AND COUNT(CASE WHEN customer_key IS NULL THEN 1 END) = 0
        THEN '✅ VALID'
        WHEN COUNT(CASE WHEN customer_key IS NULL THEN 1 END) > 0 THEN '🔴 NULL PKs FOUND'
        ELSE '🔴 DUPLICATES FOUND'
    END,
    CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER;

COMMENT ON VIEW O2C_ENH_PK_VALIDATION IS 
    'Primary key uniqueness and null validation for O2C Enhanced tables';

SELECT '✅ VIEW 10 CREATED: O2C_ENH_PK_VALIDATION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 11: FOREIGN KEY VALIDATION (Referential Integrity)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_FK_VALIDATION AS
-- Check: Reconciliation.customer_id references Customer.customer_id
SELECT
    'DM_O2C_RECONCILIATION → DIM_O2C_CUSTOMER' AS relationship,
    'customer_id' AS fk_column,
    (SELECT COUNT(DISTINCT customer_id) FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION) AS fk_distinct_values,
    (SELECT COUNT(DISTINCT customer_id) FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER) AS pk_distinct_values,
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION r
     WHERE r.customer_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER c
           WHERE c.customer_id = r.customer_id
       )
    ) AS orphan_fk_count,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION r
              WHERE r.customer_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER c
                    WHERE c.customer_id = r.customer_id
                )
             ) = 0 THEN '✅ VALID'
        ELSE '🔴 ORPHAN FKs FOUND'
    END AS fk_status,
    CURRENT_TIMESTAMP() AS validated_at

UNION ALL

-- Check: Events.customer_id references Customer.customer_id
SELECT
    'FACT_O2C_EVENTS → DIM_O2C_CUSTOMER',
    'customer_id',
    (SELECT COUNT(DISTINCT customer_id) FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS WHERE customer_id IS NOT NULL),
    (SELECT COUNT(DISTINCT customer_id) FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER),
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS e
     WHERE e.customer_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER c
           WHERE c.customer_id = e.customer_id
       )
    ),
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS e
              WHERE e.customer_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER c
                    WHERE c.customer_id = e.customer_id
                )
             ) = 0 THEN '✅ VALID'
        ELSE '🔴 ORPHAN FKs FOUND'
    END,
    CURRENT_TIMESTAMP()

UNION ALL

-- Check: Aggregates.customer_key references Customer.customer_key
SELECT
    'AGG_O2C_BY_CUSTOMER → DIM_O2C_CUSTOMER',
    'customer_key',
    (SELECT COUNT(DISTINCT customer_key) FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER),
    (SELECT COUNT(DISTINCT customer_key) FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER),
    (SELECT COUNT(*) 
     FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER a
     WHERE NOT EXISTS (
           SELECT 1 FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER c
           WHERE c.customer_key = a.customer_key
       )
    ),
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER a
              WHERE NOT EXISTS (
                    SELECT 1 FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER c
                    WHERE c.customer_key = a.customer_key
                )
             ) = 0 THEN '✅ VALID'
        ELSE '🔴 ORPHAN FKs FOUND'
    END,
    CURRENT_TIMESTAMP();

COMMENT ON VIEW O2C_ENH_FK_VALIDATION IS 
    'Foreign key referential integrity validation for O2C Enhanced tables';

SELECT '✅ VIEW 11 CREATED: O2C_ENH_FK_VALIDATION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 12: DUPLICATE DETECTION
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DUPLICATE_DETECTION AS
-- Check for potential business key duplicates in Reconciliation
WITH recon_dupes AS (
    SELECT 
        order_key,
        invoice_key,
        payment_key,
        COUNT(*) AS occurrence_count
    FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
    GROUP BY order_key, invoice_key, payment_key
    HAVING COUNT(*) > 1
)
SELECT
    'DM_O2C_RECONCILIATION' AS table_name,
    'order_key + invoice_key + payment_key' AS business_key,
    COUNT(*) AS duplicate_groups,
    SUM(occurrence_count) AS total_duplicate_rows,
    MAX(occurrence_count) AS max_duplicates_per_key,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ NO DUPLICATES'
        WHEN MAX(occurrence_count) <= 2 THEN '🟡 MINOR DUPLICATES'
        ELSE '🔴 SIGNIFICANT DUPLICATES'
    END AS duplicate_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM recon_dupes

UNION ALL

-- Check for customer duplicates by source_customer_id
SELECT
    'DIM_O2C_CUSTOMER',
    'source_customer_id',
    COUNT(*),
    SUM(cnt),
    MAX(cnt),
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ NO DUPLICATES'
        WHEN MAX(cnt) <= 2 THEN '🟡 MINOR DUPLICATES'
        ELSE '🔴 SIGNIFICANT DUPLICATES'
    END,
    CURRENT_TIMESTAMP()
FROM (
    SELECT customer_id, COUNT(*) AS cnt
    FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER
    GROUP BY customer_id
    HAVING COUNT(*) > 1
);

COMMENT ON VIEW O2C_ENH_DUPLICATE_DETECTION IS 
    'Duplicate detection for business keys in O2C Enhanced tables';

SELECT '✅ VIEW 12 CREATED: O2C_ENH_DUPLICATE_DETECTION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 13: NULL RATE TREND ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_NULL_TREND_ANALYSIS AS
-- Analyze null rates for key columns
SELECT 
    'DM_O2C_RECONCILIATION' AS table_name,
    'customer_name' AS column_name,
    'Critical' AS column_importance,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS null_rate_pct,
    CASE 
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) = 0 THEN '✅ COMPLETE'
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 1 THEN '🟢 EXCELLENT (<1%)'
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 5 THEN '🟡 ACCEPTABLE (<5%)'
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 20 THEN '🟠 HIGH (5-20%)'
        ELSE '🔴 CRITICAL (>20%)'
    END AS quality_status,
    CURRENT_TIMESTAMP() AS analyzed_at
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL

SELECT 'DM_O2C_RECONCILIATION', 'order_amount', 'Critical', COUNT(*),
    SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE 
        WHEN SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) = 0 THEN '✅ COMPLETE'
        WHEN SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 1 THEN '🟢 EXCELLENT'
        WHEN SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 5 THEN '🟡 ACCEPTABLE'
        ELSE '🔴 HIGH NULLS'
    END, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL

SELECT 'DM_O2C_RECONCILIATION', 'days_order_to_cash', 'Important', COUNT(*),
    SUM(CASE WHEN days_order_to_cash IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN days_order_to_cash IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE 
        WHEN SUM(CASE WHEN days_order_to_cash IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) < 50 THEN '✅ EXPECTED (unpaid orders)'
        ELSE '🟡 HIGH - Review'
    END, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL

SELECT 'DIM_O2C_CUSTOMER', 'customer_name', 'Critical', COUNT(*),
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CASE 
        WHEN SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) = 0 THEN '✅ COMPLETE'
        ELSE '🔴 NULLS IN DIMENSION'
    END, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER

ORDER BY 
    CASE quality_status
        WHEN '🔴 CRITICAL (>20%)' THEN 1
        WHEN '🔴 HIGH NULLS' THEN 2
        WHEN '🔴 NULLS IN DIMENSION' THEN 3
        WHEN '🟠 HIGH (5-20%)' THEN 4
        WHEN '🟡 ACCEPTABLE (<5%)' THEN 5
        ELSE 6
    END;

COMMENT ON VIEW O2C_ENH_NULL_TREND_ANALYSIS IS 
    'Null rate analysis for critical columns with quality indicators';

SELECT '✅ VIEW 13 CREATED: O2C_ENH_NULL_TREND_ANALYSIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 14: DATA CONSISTENCY (Cross-Table Validation)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_DATA_CONSISTENCY AS
WITH source_counts AS (
    SELECT 'Orders' AS entity, COUNT(*) AS source_count FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
    UNION ALL
    SELECT 'Invoices', COUNT(*) FROM EDW.CORP_TRAN.FACT_INVOICES
    UNION ALL
    SELECT 'Payments', COUNT(*) FROM EDW.CORP_TRAN.FACT_PAYMENTS
    UNION ALL
    SELECT 'Customers', COUNT(*) FROM EDW.CORP_MASTER.DIM_CUSTOMER
),
staging_counts AS (
    SELECT 'Orders' AS entity, COUNT(*) AS staging_count FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS
    UNION ALL
    SELECT 'Invoices', COUNT(*) FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES
    UNION ALL
    SELECT 'Payments', COUNT(*) FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS
),
mart_counts AS (
    SELECT 'Customers' AS entity, COUNT(*) AS mart_count FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER
    UNION ALL
    SELECT 'Reconciliation', COUNT(*) FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
)
SELECT
    'SOURCE → STAGING' AS validation_layer,
    s.entity,
    s.source_count,
    st.staging_count AS target_count,
    st.staging_count - s.source_count AS variance,
    ROUND((st.staging_count - s.source_count) * 100.0 / NULLIF(s.source_count, 0), 2) AS variance_pct,
    CASE 
        WHEN s.source_count = st.staging_count THEN '✅ MATCHED'
        WHEN ABS(st.staging_count - s.source_count) * 100.0 / NULLIF(s.source_count, 0) < 1 THEN '🟢 MINOR VARIANCE (<1%)'
        WHEN ABS(st.staging_count - s.source_count) * 100.0 / NULLIF(s.source_count, 0) < 5 THEN '🟡 MODERATE VARIANCE (1-5%)'
        ELSE '🔴 SIGNIFICANT VARIANCE (>5%)'
    END AS consistency_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM source_counts s
LEFT JOIN staging_counts st ON s.entity = st.entity
WHERE st.staging_count IS NOT NULL

UNION ALL

SELECT
    'SOURCE → MART' AS validation_layer,
    s.entity,
    s.source_count,
    m.mart_count,
    m.mart_count - s.source_count,
    ROUND((m.mart_count - s.source_count) * 100.0 / NULLIF(s.source_count, 0), 2),
    CASE 
        WHEN s.source_count = m.mart_count THEN '✅ MATCHED'
        WHEN ABS(m.mart_count - s.source_count) * 100.0 / NULLIF(s.source_count, 0) < 1 THEN '🟢 MINOR VARIANCE'
        WHEN ABS(m.mart_count - s.source_count) * 100.0 / NULLIF(s.source_count, 0) < 5 THEN '🟡 MODERATE VARIANCE'
        ELSE '🔴 SIGNIFICANT VARIANCE'
    END,
    CURRENT_TIMESTAMP()
FROM source_counts s
LEFT JOIN mart_counts m ON s.entity = m.entity
WHERE m.mart_count IS NOT NULL

ORDER BY 
    CASE consistency_status
        WHEN '🔴 SIGNIFICANT VARIANCE (>5%)' THEN 1
        WHEN '🔴 SIGNIFICANT VARIANCE' THEN 2
        WHEN '🟡 MODERATE VARIANCE (1-5%)' THEN 3
        WHEN '🟡 MODERATE VARIANCE' THEN 4
        ELSE 5
    END;

COMMENT ON VIEW O2C_ENH_DATA_CONSISTENCY IS 
    'Cross-table row count consistency validation';

SELECT '✅ VIEW 14 CREATED: O2C_ENH_DATA_CONSISTENCY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 15: DATA INTEGRITY ALERT VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_DATA_INTEGRITY AS
-- PK Violations
SELECT
    'PK_VIOLATION' AS alert_type,
    table_name,
    pk_column AS affected_column,
    pk_status AS issue,
    CASE 
        WHEN null_pk_count > 0 THEN 'CRITICAL'
        WHEN duplicate_count > 0 THEN 'HIGH'
        ELSE 'LOW'
    END AS severity,
    'Duplicates: ' || duplicate_count || ', Null PKs: ' || null_pk_count AS alert_description,
    validated_at AS detected_at
FROM O2C_ENH_PK_VALIDATION
WHERE pk_status != '✅ VALID'

UNION ALL

-- FK Violations (Orphan Records)
SELECT
    'FK_VIOLATION',
    relationship,
    fk_column,
    fk_status,
    'HIGH',
    'Orphan FK count: ' || orphan_fk_count,
    validated_at
FROM O2C_ENH_FK_VALIDATION
WHERE fk_status != '✅ VALID'

UNION ALL

-- Duplicate Issues
SELECT
    'DUPLICATES',
    table_name,
    business_key,
    duplicate_status,
    CASE 
        WHEN duplicate_status LIKE '%SIGNIFICANT%' THEN 'HIGH'
        ELSE 'MEDIUM'
    END,
    'Duplicate groups: ' || duplicate_groups || ', Total rows: ' || total_duplicate_rows,
    checked_at
FROM O2C_ENH_DUPLICATE_DETECTION
WHERE duplicate_status != '✅ NO DUPLICATES'

UNION ALL

-- High Null Rates
SELECT
    'HIGH_NULL_RATE',
    table_name,
    column_name,
    quality_status,
    CASE 
        WHEN quality_status LIKE '%CRITICAL%' THEN 'HIGH'
        WHEN quality_status LIKE '%HIGH%' THEN 'MEDIUM'
        ELSE 'LOW'
    END,
    'Null rate: ' || null_rate_pct || '% (' || null_count || ' nulls out of ' || total_rows || ')',
    analyzed_at
FROM O2C_ENH_NULL_TREND_ANALYSIS
WHERE quality_status LIKE '%🔴%' OR quality_status LIKE '%🟠%'

UNION ALL

-- Data Consistency Issues
SELECT
    'CONSISTENCY_ISSUE',
    entity,
    validation_layer,
    consistency_status,
    CASE 
        WHEN consistency_status LIKE '%SIGNIFICANT%' THEN 'HIGH'
        WHEN consistency_status LIKE '%MODERATE%' THEN 'MEDIUM'
        ELSE 'LOW'
    END,
    'Source: ' || source_count || ', Target: ' || target_count || ', Variance: ' || variance_pct || '%',
    checked_at
FROM O2C_ENH_DATA_CONSISTENCY
WHERE consistency_status NOT LIKE '%MATCHED%' AND consistency_status NOT LIKE '%MINOR%'

ORDER BY 
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END,
    detected_at DESC;

COMMENT ON VIEW O2C_ENH_ALERT_DATA_INTEGRITY IS 
    'Data integrity alerts combining PK, FK, duplicate, null, and consistency issues';

SELECT '✅ VIEW 15 CREATED: O2C_ENH_ALERT_DATA_INTEGRITY' AS status;

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
SELECT '✅ SCHEMA DRIFT, DBT & DATA INTEGRITY MONITORING COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- List all views created in this script
SELECT 
    TABLE_NAME AS view_name,
    COMMENT AS description
FROM EDW.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%SCHEMA%' 
       OR TABLE_NAME LIKE '%DDL%'
       OR TABLE_NAME LIKE '%COLUMN%'
       OR TABLE_NAME LIKE '%DBT%'
       OR TABLE_NAME LIKE '%PK%'
       OR TABLE_NAME LIKE '%FK%'
       OR TABLE_NAME LIKE '%DUPLICATE%'
       OR TABLE_NAME LIKE '%NULL%'
       OR TABLE_NAME LIKE '%CONSISTENCY%'
       OR TABLE_NAME LIKE '%INTEGRITY%'
       OR TABLE_NAME LIKE '%COVERAGE%'
       OR TABLE_NAME LIKE '%ORPHAN%')
ORDER BY TABLE_NAME;

