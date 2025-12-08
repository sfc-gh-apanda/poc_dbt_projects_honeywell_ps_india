/*
================================================================================
    DYNAMIC WAREHOUSE CONFIGURATION SETUP
================================================================================
    
    Purpose: Enable metadata-driven warehouse configuration for dbt models
    
    Benefits:
    - Change warehouse without code changes
    - No Git commits or deployments needed
    - Built-in audit trail
    - Hierarchical fallback (Model → Layer → Project → Default → profiles.yml)
    
    Setup Steps:
    1. Run this script to create config tables
    2. The dbt macro will automatically read from these tables
    3. To change warehouse: UPDATE the config table, then re-run dbt
    
================================================================================
*/

-- ============================================================================
-- STEP 1: Create Configuration Schema
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Or your admin role
USE DATABASE EDW;

CREATE SCHEMA IF NOT EXISTS EDW.CONFIG;

GRANT USAGE ON SCHEMA EDW.CONFIG TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON SCHEMA EDW.CONFIG TO ROLE DBT_O2C_PROD;

-- ============================================================================
-- STEP 2: Create Main Configuration Table
-- ============================================================================

CREATE OR REPLACE TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG (
    -- Hierarchical scope
    config_scope    VARCHAR(50) NOT NULL,   -- 'MODEL', 'LAYER', 'PROJECT', 'ENVIRONMENT', 'DEFAULT'
    scope_name      VARCHAR(200) NOT NULL,  -- The identifier for that scope
    
    -- Configuration
    warehouse_name  VARCHAR(100) NOT NULL,
    
    -- Priority (lower number = higher priority)
    -- MODEL=10, LAYER=30, PROJECT=40, ENVIRONMENT=50, DEFAULT=100
    priority        INTEGER NOT NULL DEFAULT 100,
    
    -- Validity
    is_active       BOOLEAN DEFAULT TRUE,
    effective_from  DATE DEFAULT CURRENT_DATE(),
    effective_to    DATE,                   -- NULL = no end date
    
    -- Audit
    notes           VARCHAR(500),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by      VARCHAR(100) DEFAULT CURRENT_USER(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by      VARCHAR(100) DEFAULT CURRENT_USER(),
    
    -- Constraints
    PRIMARY KEY (config_scope, scope_name)
);

-- Add comments for documentation
COMMENT ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG IS 
    'Metadata-driven warehouse configuration for dbt models. Supports hierarchical lookup with fallback.';

COMMENT ON COLUMN EDW.CONFIG.DBT_WAREHOUSE_CONFIG.config_scope IS 
    'Scope level: MODEL (most specific), LAYER, PROJECT, ENVIRONMENT, DEFAULT (least specific)';

COMMENT ON COLUMN EDW.CONFIG.DBT_WAREHOUSE_CONFIG.priority IS 
    'Lower number = higher priority. MODEL=10, LAYER=30, PROJECT=40, ENV=50, DEFAULT=100';

-- ============================================================================
-- STEP 3: Create History Table for Audit Trail
-- ============================================================================

CREATE OR REPLACE TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY (
    history_id      INTEGER AUTOINCREMENT,
    config_scope    VARCHAR(50),
    scope_name      VARCHAR(200),
    old_warehouse   VARCHAR(100),
    new_warehouse   VARCHAR(100),
    action          VARCHAR(20),            -- 'INSERT', 'UPDATE', 'DELETE'
    changed_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    changed_by      VARCHAR(100) DEFAULT CURRENT_USER(),
    notes           VARCHAR(500),
    PRIMARY KEY (history_id)
);

COMMENT ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY IS 
    'Audit trail for all warehouse configuration changes';

-- ============================================================================
-- STEP 4: Create Stored Procedure for Safe Updates (with history logging)
-- ============================================================================

CREATE OR REPLACE PROCEDURE EDW.CONFIG.UPDATE_WAREHOUSE_CONFIG(
    p_config_scope VARCHAR,
    p_scope_name VARCHAR,
    p_new_warehouse VARCHAR,
    p_notes VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_old_warehouse VARCHAR;
    v_exists BOOLEAN;
BEGIN
    -- Check if record exists
    SELECT warehouse_name INTO v_old_warehouse
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
    WHERE config_scope = :p_config_scope AND scope_name = :p_scope_name;
    
    v_exists := (v_old_warehouse IS NOT NULL);
    
    IF (v_exists) THEN
        -- Log to history
        INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY 
            (config_scope, scope_name, old_warehouse, new_warehouse, action, notes)
        VALUES 
            (:p_config_scope, :p_scope_name, :v_old_warehouse, :p_new_warehouse, 'UPDATE', :p_notes);
        
        -- Update current
        UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        SET warehouse_name = :p_new_warehouse,
            updated_at = CURRENT_TIMESTAMP(),
            updated_by = CURRENT_USER(),
            notes = :p_notes
        WHERE config_scope = :p_config_scope AND scope_name = :p_scope_name;
        
        RETURN 'Updated ' || :p_scope_name || ': ' || :v_old_warehouse || ' → ' || :p_new_warehouse;
    ELSE
        RETURN 'ERROR: No config found for scope=' || :p_config_scope || ', name=' || :p_scope_name;
    END IF;
END;
$$;

-- ============================================================================
-- STEP 5: Create Procedure to Add New Config
-- ============================================================================

CREATE OR REPLACE PROCEDURE EDW.CONFIG.ADD_WAREHOUSE_CONFIG(
    p_config_scope VARCHAR,
    p_scope_name VARCHAR,
    p_warehouse VARCHAR,
    p_priority INTEGER,
    p_notes VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert new config
    INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
        (config_scope, scope_name, warehouse_name, priority, notes)
    VALUES 
        (:p_config_scope, :p_scope_name, :p_warehouse, :p_priority, :p_notes);
    
    -- Log to history
    INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY 
        (config_scope, scope_name, old_warehouse, new_warehouse, action, notes)
    VALUES 
        (:p_config_scope, :p_scope_name, NULL, :p_warehouse, 'INSERT', :p_notes);
    
    RETURN 'Added config: ' || :p_config_scope || '/' || :p_scope_name || ' → ' || :p_warehouse;
END;
$$;

-- ============================================================================
-- STEP 6: Insert Default Configuration for O2C Project
-- ============================================================================

-- Clear existing (for re-runs)
DELETE FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG WHERE config_scope IS NOT NULL;

-- Global default (catches everything)
INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    (config_scope, scope_name, warehouse_name, priority, notes)
VALUES
    ('DEFAULT', 'DEFAULT', 'COMPUTE_WH', 100, 'Global fallback for all models');

-- Environment defaults
INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    (config_scope, scope_name, warehouse_name, priority, notes)
VALUES
    ('ENVIRONMENT', 'dev', 'COMPUTE_WH', 50, 'Development environment default'),
    ('ENVIRONMENT', 'prod', 'COMPUTE_WH', 50, 'Production environment default');

-- Project defaults
INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    (config_scope, scope_name, warehouse_name, priority, notes)
VALUES
    ('PROJECT', 'dbt_o2c', 'COMPUTE_WH', 40, 'O2C project default'),
    ('PROJECT', 'dbt_o2c_enhanced', 'COMPUTE_WH', 40, 'O2C Enhanced project default'),
    ('PROJECT', 'dbt_foundation', 'COMPUTE_WH', 40, 'Foundation project default'),
    ('PROJECT', 'dbt_finance_core', 'COMPUTE_WH', 40, 'Finance project default');

-- Layer defaults
INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    (config_scope, scope_name, warehouse_name, priority, notes)
VALUES
    ('LAYER', 'staging', 'COMPUTE_WH', 30, 'Staging layer - lightweight views'),
    ('LAYER', 'marts', 'COMPUTE_WH', 30, 'Marts layer - heavier tables'),
    ('LAYER', 'snapshots', 'COMPUTE_WH', 30, 'Snapshots layer');

-- Model-specific overrides (examples - uncomment and modify as needed)
-- INSERT INTO EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
--     (config_scope, scope_name, warehouse_name, priority, notes)
-- VALUES
--     ('MODEL', 'dm_o2c_reconciliation', 'COMPUTE_WH_LARGE', 10, 'Heavy reconciliation mart'),
--     ('MODEL', 'agg_o2c_by_period', 'COMPUTE_WH_LARGE', 10, 'Time-series aggregation');

-- ============================================================================
-- STEP 7: Grant Permissions
-- ============================================================================

-- Grant to dbt roles
GRANT SELECT, INSERT, UPDATE ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE EDW.CONFIG.UPDATE_WAREHOUSE_CONFIG(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON PROCEDURE EDW.CONFIG.ADD_WAREHOUSE_CONFIG(VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR) TO ROLE DBT_O2C_DEVELOPER;

GRANT SELECT, INSERT, UPDATE ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG TO ROLE DBT_O2C_PROD;
GRANT SELECT ON TABLE EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY TO ROLE DBT_O2C_PROD;
GRANT USAGE ON PROCEDURE EDW.CONFIG.UPDATE_WAREHOUSE_CONFIG(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DBT_O2C_PROD;
GRANT USAGE ON PROCEDURE EDW.CONFIG.ADD_WAREHOUSE_CONFIG(VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR) TO ROLE DBT_O2C_PROD;

-- ============================================================================
-- STEP 8: Verify Setup
-- ============================================================================

-- View current configuration
SELECT 
    config_scope,
    scope_name,
    warehouse_name,
    priority,
    is_active,
    notes
FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
ORDER BY priority, config_scope, scope_name;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- Example 1: Change warehouse for a specific model (after a timeout)
CALL EDW.CONFIG.UPDATE_WAREHOUSE_CONFIG(
    'MODEL',                          -- scope
    'dm_o2c_reconciliation',          -- model name
    'COMPUTE_WH_LARGE',               -- new warehouse
    'Upgraded due to timeout'         -- notes
);

-- Example 2: Add model-specific config
CALL EDW.CONFIG.ADD_WAREHOUSE_CONFIG(
    'MODEL',                          -- scope
    'dm_o2c_reconciliation',          -- model name
    'COMPUTE_WH_LARGE',               -- warehouse
    10,                               -- priority (MODEL level)
    'Heavy reconciliation mart'       -- notes
);

-- Example 3: Quick update (direct SQL)
UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET warehouse_name = 'COMPUTE_WH_XLARGE',
    updated_at = CURRENT_TIMESTAMP(),
    notes = 'Upgraded due to timeout on ' || CURRENT_DATE()
WHERE scope_name = 'dm_o2c_reconciliation';

-- Example 4: View change history
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY
ORDER BY changed_at DESC;

-- Example 5: Temporarily disable a config
UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET is_active = FALSE
WHERE scope_name = 'dm_o2c_reconciliation';

*/

-- ============================================================================
-- END OF SETUP
-- ============================================================================

SELECT '✅ Warehouse configuration setup complete!' AS status;


