/*
================================================================================
    DBT LOG ARCHIVE SETUP - Persist dbt.log to Snowflake
================================================================================

    Purpose:
        Capture and persist dbt.log content to Snowflake tables for historical
        analysis, debugging, and audit trail.
    
    Features:
        - Parses dbt.log file to identify individual runs by header pattern
        - Archives only the latest run (or all unarchived runs)
        - Deduplicates by run_id (same as dbt_run_id in audit columns)
        - Extracts error/warning counts automatically
        - Links to existing audit tables (DBT_RUN_LOG, DBT_MODEL_LOG)

    SETUP STEPS:
        1. Run this entire script in Snowflake
        2. The on-run-end hook will automatically archive logs after each dbt run
        3. Query the archive table for historical log analysis

================================================================================
*/

-- ============================================================================
-- STEP 1: Create Log Archive Table
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Or your admin role
USE DATABASE EDW;
USE SCHEMA O2C_AUDIT;

CREATE TABLE IF NOT EXISTS EDW.O2C_AUDIT.DBT_LOG_ARCHIVE (
    -- Primary Key (same as dbt_run_id / invocation_id)
    run_id              VARCHAR(50) PRIMARY KEY,
    
    -- Run metadata
    project_name        VARCHAR(100),
    target_environment  VARCHAR(20),
    dbt_version         VARCHAR(20),
    dbt_command         VARCHAR(50),
    
    -- Log content
    log_content         VARCHAR(16777216),  -- 16MB max (Snowflake VARCHAR limit)
    log_size_bytes      INTEGER,
    log_line_count      INTEGER,
    
    -- Extracted metrics
    error_count         INTEGER DEFAULT 0,
    warning_count       INTEGER DEFAULT 0,
    
    -- Timestamps
    run_started_at      TIMESTAMP_NTZ,
    archived_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comment
COMMENT ON TABLE EDW.O2C_AUDIT.DBT_LOG_ARCHIVE IS 
    'Archives dbt.log content for each run. Links to DBT_RUN_LOG and DBT_MODEL_LOG via run_id.';

-- ============================================================================
-- STEP 2: Create Python Stored Procedure for Log Parsing & Archiving
-- ============================================================================

CREATE OR REPLACE PROCEDURE EDW.O2C_AUDIT.ARCHIVE_DBT_LOG(
    p_archive_mode VARCHAR DEFAULT 'LATEST'  -- 'LATEST' or 'ALL'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'archive_dbt_log'
EXECUTE AS CALLER
AS
'
import re
import os
from datetime import datetime

def archive_dbt_log(session, p_archive_mode="LATEST"):
    """
    Archive dbt.log content to EDW.O2C_AUDIT.DBT_LOG_ARCHIVE
    
    Args:
        session: Snowpark session
        p_archive_mode: LATEST (only most recent run) or ALL (all unarchived runs)
    
    Returns:
        dict with status, archived count, and details
    """
    log_path = "/tmp/dbt/logs/dbt.log"
    
    # Check if log file exists
    if not os.path.exists(log_path):
        return {
            "status": "error",
            "message": "Log file not found at " + log_path,
            "archived_count": 0
        }
    
    # Read the entire log file
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    
    if not content.strip():
        return {
            "status": "error",
            "message": "Log file is empty",
            "archived_count": 0
        }
    
    # Pattern to match run headers:
    # ============================== 00:53:51.447649 | 8f058ebd-e6a0-4ec9-8987-740d2e74e165 ==============================
    header_pattern = r"^={30} ([\\d:.]+) \\| ([a-f0-9-]{36}) ={30}$"
    
    # Find all run headers with their positions
    matches = list(re.finditer(header_pattern, content, re.MULTILINE))
    
    if not matches:
        return {
            "status": "error",
            "message": "No run headers found in log file",
            "archived_count": 0
        }
    
    # Determine which runs to archive
    if p_archive_mode.upper() == "LATEST":
        runs_to_process = [matches[-1]]  # Only the last match
        run_indices = [len(matches) - 1]
    else:  # ALL
        runs_to_process = matches
        run_indices = list(range(len(matches)))
    
    archived_runs = []
    skipped_runs = []
    
    for idx, match in zip(run_indices, runs_to_process):
        timestamp_str = match.group(1)
        run_id = match.group(2)
        
        # Check if already archived
        existing_check = session.sql(f"SELECT 1 FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE WHERE run_id = \'{run_id}\'").collect()
        
        if existing_check:
            skipped_runs.append(run_id)
            continue
        
        # Extract this runs content
        start_pos = match.start()
        # End is either the next header or EOF
        if idx + 1 < len(matches):
            end_pos = matches[idx + 1].start()
        else:
            end_pos = len(content)
        
        run_log = content[start_pos:end_pos].strip()
        
        # Extract metrics from log content
        error_count = len(re.findall(r"\\[error\\s?\\]", run_log, re.IGNORECASE))
        warning_count = len(re.findall(r"\\[warn\\s?\\]", run_log, re.IGNORECASE))
        line_count = run_log.count("\\n") + 1
        
        # Extract dbt version and command
        version_match = re.search(r"Running with dbt=(\\d+\\.\\d+\\.\\d+)", run_log)
        dbt_version = version_match.group(1) if version_match else "unknown"
        
        command_match = re.search(r"Command `cli (\\w+)`", run_log)
        dbt_command = command_match.group(1) if command_match else "unknown"
        
        # Extract target environment
        env_match = re.search(r"target=\'(\\w+)\'", run_log)
        target_env = env_match.group(1) if env_match else "unknown"
        
        # Use parameterized insert to handle special characters
        try:
            # Create a DataFrame and write to table
            from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType
            from snowflake.snowpark.functions import current_timestamp
            
            data = [(
                run_id,
                "dbt_o2c_enhanced",
                target_env,
                dbt_version,
                dbt_command,
                run_log,
                len(run_log),
                line_count,
                error_count,
                warning_count
            )]
            
            schema = StructType([
                StructField("RUN_ID", StringType()),
                StructField("PROJECT_NAME", StringType()),
                StructField("TARGET_ENVIRONMENT", StringType()),
                StructField("DBT_VERSION", StringType()),
                StructField("DBT_COMMAND", StringType()),
                StructField("LOG_CONTENT", StringType()),
                StructField("LOG_SIZE_BYTES", IntegerType()),
                StructField("LOG_LINE_COUNT", IntegerType()),
                StructField("ERROR_COUNT", IntegerType()),
                StructField("WARNING_COUNT", IntegerType())
            ])
            
            df = session.create_dataframe(data, schema)
            df = df.with_column("ARCHIVED_AT", current_timestamp())
            
            df.write.mode("append").save_as_table("EDW.O2C_AUDIT.DBT_LOG_ARCHIVE")
            
            archived_runs.append({
                "run_id": run_id,
                "bytes": len(run_log),
                "lines": line_count,
                "errors": error_count,
                "warnings": warning_count
            })
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to insert run {run_id}: {str(e)}",
                "archived_count": len(archived_runs)
            }
    
    return {
        "status": "success",
        "archived_count": len(archived_runs),
        "skipped_count": len(skipped_runs),
        "archived_runs": archived_runs,
        "skipped_runs": skipped_runs,
        "total_runs_in_file": len(matches)
    }
';

-- Add comment
COMMENT ON PROCEDURE EDW.O2C_AUDIT.ARCHIVE_DBT_LOG(VARCHAR) IS 
    'Parses dbt.log and archives run logs to DBT_LOG_ARCHIVE table. Modes: LATEST (default) or ALL.';

-- ============================================================================
-- STEP 3: Grant Permissions
-- ============================================================================

GRANT USAGE ON PROCEDURE EDW.O2C_AUDIT.ARCHIVE_DBT_LOG(VARCHAR) TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE EDW.O2C_AUDIT.ARCHIVE_DBT_LOG(VARCHAR) TO ROLE DBT_O2C_PROD;

GRANT SELECT, INSERT ON TABLE EDW.O2C_AUDIT.DBT_LOG_ARCHIVE TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT, INSERT ON TABLE EDW.O2C_AUDIT.DBT_LOG_ARCHIVE TO ROLE DBT_O2C_PROD;

-- ============================================================================
-- STEP 4: Create Helper Views
-- ============================================================================

-- View: Recent logs with summary
CREATE OR REPLACE VIEW EDW.O2C_AUDIT.V_DBT_LOG_SUMMARY AS
SELECT 
    run_id,
    project_name,
    target_environment,
    dbt_version,
    dbt_command,
    log_size_bytes,
    log_line_count,
    error_count,
    warning_count,
    archived_at,
    CASE 
        WHEN error_count > 0 THEN 'FAILED'
        WHEN warning_count > 0 THEN 'WARNING'
        ELSE 'SUCCESS'
    END AS run_status
FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE
ORDER BY archived_at DESC;

-- View: Logs with errors
CREATE OR REPLACE VIEW EDW.O2C_AUDIT.V_DBT_LOG_ERRORS AS
SELECT 
    run_id,
    archived_at,
    error_count,
    -- Extract first error line from log content
    REGEXP_SUBSTR(log_content, '\\[error\\s?\\].*', 1, 1, 'im') AS first_error
FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE
WHERE error_count > 0
ORDER BY archived_at DESC;

-- View: Combined audit (Log + Run + Model summary)
CREATE OR REPLACE VIEW EDW.O2C_AUDIT.V_DBT_RUN_COMPLETE AS
SELECT 
    COALESCE(r.run_id, l.run_id) AS run_id,
    COALESCE(r.project_name, l.project_name) AS project_name,
    r.run_status,
    r.models_run,
    r.models_success,
    r.models_failed,
    r.models_skipped,
    r.run_started_at,
    r.run_ended_at,
    r.run_duration_seconds,
    r.run_command,
    r.warehouse_name,
    l.log_size_bytes,
    l.log_line_count,
    l.error_count AS log_errors,
    l.warning_count AS log_warnings,
    l.dbt_version,
    l.dbt_command
FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
FULL OUTER JOIN EDW.O2C_AUDIT.DBT_LOG_ARCHIVE l ON r.run_id = l.run_id
ORDER BY COALESCE(r.run_started_at, l.archived_at) DESC;

-- ============================================================================
-- STEP 5: Test the Setup
-- ============================================================================

-- Test 1: Check if table exists
SELECT COUNT(*) AS row_count FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE;

-- Test 2: Call the procedure (archive latest run)
-- CALL EDW.O2C_AUDIT.ARCHIVE_DBT_LOG('LATEST');

-- Test 3: Archive all unarchived runs
-- CALL EDW.O2C_AUDIT.ARCHIVE_DBT_LOG('ALL');

-- Test 4: View summary
-- SELECT * FROM EDW.O2C_AUDIT.V_DBT_LOG_SUMMARY;

-- Test 5: View combined audit
-- SELECT * FROM EDW.O2C_AUDIT.V_DBT_RUN_COMPLETE;

-- ============================================================================
-- STEP 6: Sample Queries
-- ============================================================================

/*
-- Search for specific error in logs
SELECT run_id, archived_at
FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE
WHERE log_content ILIKE '%compilation error%'
ORDER BY archived_at DESC;

-- Get full log for a specific run
SELECT log_content
FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE
WHERE run_id = '<your-run-id>';

-- Runs with errors in last 7 days
SELECT *
FROM EDW.O2C_AUDIT.V_DBT_LOG_SUMMARY
WHERE error_count > 0
  AND archived_at >= DATEADD('day', -7, CURRENT_TIMESTAMP());

-- Average log size by command type
SELECT 
    dbt_command,
    COUNT(*) AS run_count,
    AVG(log_size_bytes) AS avg_log_bytes,
    AVG(log_line_count) AS avg_lines
FROM EDW.O2C_AUDIT.DBT_LOG_ARCHIVE
GROUP BY dbt_command
ORDER BY run_count DESC;
*/
