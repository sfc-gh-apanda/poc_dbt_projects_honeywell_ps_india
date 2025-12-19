-- ═══════════════════════════════════════════════════════════════════════════════
-- SETUP SCHEMA FOR SEMANTIC VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Create schema for semantic views before running dbt build
-- Run this ONCE before deploying semantic views
-- 
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;  -- Or your admin role
USE DATABASE EDW;
USE WAREHOUSE COMPUTE_WH;

-- Create schema for semantic views
CREATE SCHEMA IF NOT EXISTS EDW.O2C_ENHANCED_SEMANTIC_VIEWS
    COMMENT = 'Semantic views for Cortex Analyst - O2C Enhanced project';

-- Grant permissions to dbt roles
GRANT USAGE ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_PROD;

GRANT CREATE SEMANTIC VIEW ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_DEVELOPER;
GRANT CREATE SEMANTIC VIEW ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_PROD;

GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_DEVELOPER;
GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_PROD;

-- Verify
SELECT '✅ Schema created: EDW.O2C_ENHANCED_SEMANTIC_VIEWS' AS status;
SHOW GRANTS ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS;

