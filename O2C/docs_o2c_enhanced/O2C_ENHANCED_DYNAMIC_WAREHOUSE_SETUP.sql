/*
================================================================================
    DYNAMIC WAREHOUSE SETUP - Stored Procedure Approach
================================================================================

    Purpose: 
        Enable dynamic warehouse switching per model via stored procedure.
        The stored procedure executes USE WAREHOUSE internally, bypassing
        Snowflake's limitation that USE WAREHOUSE doesn't accept dynamic expressions.

    SETUP STEPS (Run ONCE before using dbt):
        1. Run this entire script in Snowflake
        2. Verify the stored procedure works with test queries at the bottom
        3. Then run dbt - the pre_hook will call the stored procedure

    TO CHANGE WAREHOUSE (No CI/CD needed):
        Just UPDATE the config table:
        
        UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        SET warehouse_name = 'COMPUTE_WH_LARGE'
        WHERE scope_name = 'dbt_o2c_enhanced';

================================================================================
*/

-- ============================================================================
-- STEP 1: Create Config Schema (if not exists)
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Or your admin role
USE DATABASE EDW;

CREATE SCHEMA IF NOT EXISTS EDW.CONFIG;

-- ============================================================================
-- STEP 2: Create Config Table (if not exists)
-- ============================================================================

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

-- ============================================================================
-- STEP 3: Create the Dynamic Warehouse Stored Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(
    p_model_name VARCHAR,
    p_layer_name VARCHAR DEFAULT NULL,
    p_project_name VARCHAR DEFAULT NULL,
    p_environment VARCHAR DEFAULT 'dev'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER  -- Runs in caller's session context (required for USE WAREHOUSE)
AS
$$
DECLARE
    v_warehouse VARCHAR;
    v_scopes ARRAY;
BEGIN
    -- Build array of scopes to check (filter out NULLs and empty strings)
    v_scopes := ARRAY_CONSTRUCT_COMPACT(
        :p_model_name,
        :p_layer_name,
        :p_project_name,
        :p_environment,
        'DEFAULT'
    );
    
    -- Look up warehouse with priority ordering
    SELECT warehouse_name INTO v_warehouse
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
    WHERE is_active = TRUE
      AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
      AND scope_name IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => :v_scopes)))
    ORDER BY priority ASC
    LIMIT 1;
    
    -- Fallback if not found in config table
    IF (v_warehouse IS NULL) THEN
        v_warehouse := 'COMPUTE_WH';
    END IF;
    
    -- Execute USE WAREHOUSE dynamically
    EXECUTE IMMEDIATE 'USE WAREHOUSE ' || v_warehouse;
    
    -- Return confirmation message
    RETURN 'Switched to warehouse: ' || v_warehouse;
    
EXCEPTION
    WHEN OTHER THEN
        -- If anything fails, try to use default warehouse
        BEGIN
            EXECUTE IMMEDIATE 'USE WAREHOUSE COMPUTE_WH';
            RETURN 'Fallback to COMPUTE_WH due to error: ' || SQLERRM;
        EXCEPTION
            WHEN OTHER THEN
                RETURN 'ERROR: ' || SQLERRM;
        END;
END;
$$;

-- Add comment for documentation
COMMENT ON PROCEDURE EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS 
    'Dynamically switches warehouse based on config table lookup. Used by dbt pre_hook for per-model warehouse assignment.';

-- ============================================================================
-- STEP 4: Insert Default Configuration
-- ============================================================================

-- Clear and re-insert defaults (idempotent)
MERGE INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG t
USING (
    SELECT 'DEFAULT' as config_scope, 'DEFAULT' as scope_name, 'COMPUTE_WH' as warehouse_name, 100 as priority, 'Global fallback' as notes
    UNION ALL
    SELECT 'ENVIRONMENT', 'dev', 'COMPUTE_WH', 50, 'Development environment'
    UNION ALL
    SELECT 'ENVIRONMENT', 'prod', 'COMPUTE_WH', 50, 'Production environment'
    UNION ALL
    SELECT 'PROJECT', 'dbt_o2c_enhanced', 'COMPUTE_WH', 40, 'O2C Enhanced project'
    UNION ALL
    SELECT 'LAYER', 'staging', 'COMPUTE_WH', 30, 'Staging layer - views'
    UNION ALL
    SELECT 'LAYER', 'marts', 'COMPUTE_WH', 30, 'Marts layer - tables'
) s
ON t.config_scope = s.config_scope AND t.scope_name = s.scope_name
WHEN NOT MATCHED THEN 
    INSERT (config_scope, scope_name, warehouse_name, priority, notes)
    VALUES (s.config_scope, s.scope_name, s.warehouse_name, s.priority, s.notes);

-- ============================================================================
-- STEP 5: Grant Permissions
-- ============================================================================

GRANT USAGE ON SCHEMA EDW.CONFIG TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON SCHEMA EDW.CONFIG TO ROLE DBT_O2C_PROD;

GRANT SELECT, INSERT, UPDATE ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT, INSERT, UPDATE ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG TO ROLE DBT_O2C_PROD;

GRANT USAGE ON PROCEDURE EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_PROD;

-- ============================================================================
-- STEP 6: Verification Tests
-- ============================================================================

-- Test 1: View current configuration
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG ORDER BY priority;

-- Test 2: Test the stored procedure (should return "Switched to warehouse: COMPUTE_WH")
CALL EDW.CONFIG.SET_DYNAMIC_WAREHOUSE('dim_o2c_customer', 'marts', 'dbt_o2c_enhanced', 'dev');

-- Test 3: Verify current warehouse after the call
SELECT CURRENT_WAREHOUSE();

-- ============================================================================
-- EXAMPLE: Change warehouse for a specific model
-- ============================================================================

/*
-- To test dynamic switching, add a model-specific override:

INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    (config_scope, scope_name, warehouse_name, priority, notes)
VALUES 
    ('MODEL', 'dim_o2c_customer', 'HONEYWELL_POC', 10, 'Test model-specific warehouse');

-- Now when dbt runs dim_o2c_customer, it will use HONEYWELL_POC
-- To verify:
CALL EDW.CONFIG.SET_DYNAMIC_WAREHOUSE('dim_o2c_customer', 'marts', 'dbt_o2c_enhanced', 'dev');
SELECT CURRENT_WAREHOUSE();  -- Should show HONEYWELL_POC

-- Clean up:
DELETE FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
WHERE config_scope = 'MODEL' AND scope_name = 'dim_o2c_customer';
*/

