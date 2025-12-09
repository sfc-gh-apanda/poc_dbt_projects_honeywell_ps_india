/*
================================================================================
    DYNAMIC WAREHOUSE EXECUTION FOR DBT
================================================================================

    Purpose: 
        Allow users to change warehouse without CI/CD or code changes.
        The warehouse is read from a config table BEFORE dbt executes.

    How It Works:
        1. User calls stored procedure: CALL RUN_DBT_WITH_DYNAMIC_WAREHOUSE(...)
        2. Procedure reads warehouse from EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        3. Procedure switches to that warehouse using USE WAREHOUSE
        4. Procedure executes dbt build
        5. Warehouse is determined at RUNTIME, not compile time

    To Change Warehouse:
        Just UPDATE the config table - no CI/CD needed!
        
        UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        SET warehouse_name = 'COMPUTE_WH_LARGE'
        WHERE scope_name = 'dbt_o2c_enhanced';

================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ============================================================================
-- STEP 1: Ensure Config Table Exists (from O2C_WAREHOUSE_CONFIG_SETUP.sql)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS EDW.CONFIG;

CREATE TABLE IF NOT EXISTS EDW.CONFIG.DBT_WAREHOUSE_CONFIG (
    config_scope    VARCHAR(50) NOT NULL,
    scope_name      VARCHAR(200) NOT NULL,
    warehouse_name  VARCHAR(100) NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 100,
    is_active       BOOLEAN DEFAULT TRUE,
    effective_from  DATE DEFAULT CURRENT_DATE(),
    effective_to    DATE,
    notes           VARCHAR(500),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by      VARCHAR(100) DEFAULT CURRENT_USER(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by      VARCHAR(100) DEFAULT CURRENT_USER(),
    PRIMARY KEY (config_scope, scope_name)
);

-- Insert default config if not exists
MERGE INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG t
USING (SELECT 'PROJECT' as config_scope, 'dbt_o2c_enhanced' as scope_name, 
              'COMPUTE_WH' as warehouse_name, 40 as priority) s
ON t.config_scope = s.config_scope AND t.scope_name = s.scope_name
WHEN NOT MATCHED THEN INSERT (config_scope, scope_name, warehouse_name, priority)
VALUES (s.config_scope, s.scope_name, s.warehouse_name, s.priority);

-- ============================================================================
-- STEP 2: Create Stored Procedure to Run dbt with Dynamic Warehouse
-- ============================================================================

CREATE OR REPLACE PROCEDURE EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(
    p_project_name VARCHAR,           -- e.g., 'dbt_o2c_enhanced'
    p_dbt_args VARCHAR DEFAULT 'build --target dev',  -- e.g., 'build --select dim_o2c_customer'
    p_git_repo VARCHAR DEFAULT 'USER$.PUBLIC.poc_dbt_projects_honeywell_ps_india',
    p_project_root VARCHAR DEFAULT '/O2C/dbt_o2c_enhanced'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_warehouse VARCHAR;
    v_target VARCHAR;
    v_dbt_command VARCHAR;
    v_result VARCHAR;
BEGIN
    -- Extract target from dbt_args (default to 'dev')
    v_target := COALESCE(
        REGEXP_SUBSTR(:p_dbt_args, '--target\\s+(\\w+)', 1, 1, 'e', 1),
        'dev'
    );
    
    -- Look up warehouse from config table with priority
    SELECT warehouse_name INTO v_warehouse
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
    WHERE is_active = TRUE
      AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
      AND scope_name IN (:p_project_name, :v_target, 'DEFAULT')
    ORDER BY priority ASC
    LIMIT 1;
    
    -- Fallback to default if not found
    IF (v_warehouse IS NULL) THEN
        v_warehouse := 'COMPUTE_WH';
    END IF;
    
    -- Log the warehouse being used
    SYSTEM$LOG_INFO('Dynamic Warehouse: Using ' || v_warehouse || ' for project ' || :p_project_name);
    
    -- Switch to the dynamic warehouse
    EXECUTE IMMEDIATE 'USE WAREHOUSE ' || v_warehouse;
    
    -- Sync the git repository
    EXECUTE IMMEDIATE 'ALTER GIT REPOSITORY ' || :p_git_repo || ' FETCH';
    
    -- Build the dbt command
    v_dbt_command := 'EXECUTE DBT PROJECT FROM WORKSPACE ' || :p_git_repo || 
                     ' PROJECT_ROOT=''' || :p_project_root || '''' ||
                     ' ARGS=''' || :p_dbt_args || '''';
    
    -- Execute dbt
    EXECUTE IMMEDIATE v_dbt_command;
    
    v_result := 'SUCCESS: dbt executed on warehouse ' || v_warehouse;
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- STEP 3: Create Helper Procedures
-- ============================================================================

-- Procedure to update warehouse config
CREATE OR REPLACE PROCEDURE EDW.CONFIG.SET_PROJECT_WAREHOUSE(
    p_project_name VARCHAR,
    p_warehouse_name VARCHAR,
    p_notes VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
    SET warehouse_name = :p_warehouse_name,
        updated_at = CURRENT_TIMESTAMP(),
        updated_by = CURRENT_USER(),
        notes = COALESCE(:p_notes, notes)
    WHERE scope_name = :p_project_name;
    
    IF (ROW_COUNT() = 0) THEN
        INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
            (config_scope, scope_name, warehouse_name, priority, notes)
        VALUES 
            ('PROJECT', :p_project_name, :p_warehouse_name, 40, :p_notes);
    END IF;
    
    RETURN 'Warehouse for ' || :p_project_name || ' set to ' || :p_warehouse_name;
END;
$$;

-- Procedure to view current config
CREATE OR REPLACE PROCEDURE EDW.CONFIG.GET_WAREHOUSE_CONFIG()
RETURNS TABLE (
    config_scope VARCHAR,
    scope_name VARCHAR,
    warehouse_name VARCHAR,
    priority INTEGER,
    is_active BOOLEAN
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (SELECT config_scope, scope_name, warehouse_name, priority, is_active
            FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
            WHERE is_active = TRUE
            ORDER BY priority);
    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- STEP 4: Grant Permissions
-- ============================================================================

GRANT USAGE ON SCHEMA EDW.CONFIG TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON SCHEMA EDW.CONFIG TO ROLE DBT_O2C_PROD;

GRANT SELECT, INSERT, UPDATE ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT, INSERT, UPDATE ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG TO ROLE DBT_O2C_PROD;

GRANT USAGE ON PROCEDURE EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE EDW.CONFIG.SET_PROJECT_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE EDW.CONFIG.GET_WAREHOUSE_CONFIG() TO ROLE DBT_O2C_DEVELOPER;

GRANT USAGE ON PROCEDURE EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_PROD;
GRANT USAGE ON PROCEDURE EDW.CONFIG.SET_PROJECT_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_PROD;
GRANT USAGE ON PROCEDURE EDW.CONFIG.GET_WAREHOUSE_CONFIG() TO ROLE DBT_O2C_PROD;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- ═══════════════════════════════════════════════════════════════════════════
-- EXAMPLE 1: Run dbt build with dynamic warehouse
-- ═══════════════════════════════════════════════════════════════════════════

CALL EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(
    'dbt_o2c_enhanced',                              -- project name
    'build --full-refresh --target dev',             -- dbt args
    'USER$.PUBLIC.poc_dbt_projects_honeywell_ps_india',  -- git repo
    '/O2C/dbt_o2c_enhanced'                          -- project root
);


-- ═══════════════════════════════════════════════════════════════════════════
-- EXAMPLE 2: Run specific models
-- ═══════════════════════════════════════════════════════════════════════════

CALL EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(
    'dbt_o2c_enhanced',
    'run --select dim_o2c_customer dm_o2c_reconciliation --target dev'
);


-- ═══════════════════════════════════════════════════════════════════════════
-- EXAMPLE 3: Change warehouse WITHOUT any code/CI/CD changes
-- ═══════════════════════════════════════════════════════════════════════════

-- Step 1: View current config
CALL EDW.CONFIG.GET_WAREHOUSE_CONFIG();

-- Step 2: Switch to larger warehouse
CALL EDW.CONFIG.SET_PROJECT_WAREHOUSE(
    'dbt_o2c_enhanced', 
    'COMPUTE_WH_LARGE',
    'Switching to large WH for heavy load'
);

-- Step 3: Run dbt (will automatically use COMPUTE_WH_LARGE)
CALL EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(
    'dbt_o2c_enhanced',
    'build --target dev'
);

-- Step 4: Switch back to normal warehouse after heavy load
CALL EDW.CONFIG.SET_PROJECT_WAREHOUSE(
    'dbt_o2c_enhanced', 
    'COMPUTE_WH',
    'Switching back to normal WH'
);


-- ═══════════════════════════════════════════════════════════════════════════
-- EXAMPLE 4: Direct SQL to change warehouse (alternative to procedure)
-- ═══════════════════════════════════════════════════════════════════════════

UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET warehouse_name = 'HONEYWELL_POC',
    updated_at = CURRENT_TIMESTAMP(),
    updated_by = CURRENT_USER(),
    notes = 'Testing with HONEYWELL_POC warehouse'
WHERE scope_name = 'dbt_o2c_enhanced';


-- ═══════════════════════════════════════════════════════════════════════════
-- EXAMPLE 5: Verify which warehouse will be used
-- ═══════════════════════════════════════════════════════════════════════════

SELECT 
    scope_name as project,
    warehouse_name as current_warehouse,
    notes as last_change_reason,
    updated_at as last_changed,
    updated_by as changed_by
FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
WHERE scope_name = 'dbt_o2c_enhanced';

*/

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show current config
SELECT 
    config_scope,
    scope_name,
    warehouse_name,
    priority,
    is_active,
    notes
FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
ORDER BY priority, config_scope;

