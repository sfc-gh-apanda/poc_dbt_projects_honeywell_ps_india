-- ============================================================================
-- Data Preparation Script for DBT Foundation & Finance Core (AR Aging Branch)
-- ============================================================================
-- Purpose: Create and populate all required tables for the smallest isolated branch
-- Branch: Source → Foundation → Finance (AR Aging)
-- Run this in Snowflake before running DBT
-- ============================================================================

-- Set context
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- STEP 1: Create Databases and Schemas
-- ============================================================================

-- Create EDW database structure (if not exists)
CREATE DATABASE IF NOT EXISTS EDW;

-- Raw/Transaction layer
CREATE SCHEMA IF NOT EXISTS EDW.CORP_TRAN COMMENT = 'Transaction fact tables from source systems';

-- Master data layer
CREATE SCHEMA IF NOT EXISTS EDW.CORP_MASTER COMMENT = 'Master dimension tables';

-- Reference data layer
CREATE SCHEMA IF NOT EXISTS EDW.CORP_REF COMMENT = 'Reference and lookup tables';

-- DBT development schemas (per developer)
CREATE SCHEMA IF NOT EXISTS EDW.DEV_DBT COMMENT = 'DBT development workspace';

-- DBT staging schema (foundation output)
CREATE SCHEMA IF NOT EXISTS EDW.DBT_STAGING COMMENT = 'Foundation project staging models';

-- DBT shared schema (foundation output)
CREATE SCHEMA IF NOT EXISTS EDW.DBT_SHARED COMMENT = 'Foundation project shared dimensions';

-- DBT data mart schema (finance output)
CREATE SCHEMA IF NOT EXISTS EDW.CORP_DM_FIN COMMENT = 'Finance data marts';

-- ============================================================================
-- STEP 2: Create Source Tables (Raw Layer)
-- ============================================================================

-- Create FACT_ACCOUNT_RECEIVABLE_GBL (simplified for POC)
CREATE OR REPLACE TABLE EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL (
    -- Primary Keys
    SOURCE_SYSTEM VARCHAR(50) NOT NULL,
    COMPANY_CODE VARCHAR(50) NOT NULL,
    ACCOUNTING_DOC VARCHAR(50) NOT NULL,
    ACCOUNT_DOC_LINE_ITEM VARCHAR(10) NOT NULL,
    FISCAL_YEAR VARCHAR(4) NOT NULL,
    
    -- Amounts
    AMT_DOC NUMBER(23,3),
    AMT_USD NUMBER(23,3),
    AMT_USD_ME NUMBER(23,3),
    AMT_LCL NUMBER(23,3),
    
    -- Currency
    DOC_CURR VARCHAR(5),
    LCL_CURR VARCHAR(5),
    
    -- Dates
    DOC_DATE DATE,
    POSTING_DATE DATE,
    NET_DUE_DATE DATE,
    BASELINE_DATE DATE,
    CLEARING_DATE DATE,
    
    -- Customer
    SOLD_TO VARCHAR(50),
    CUSTOMER_NUM_SK VARCHAR(50),
    
    -- GL and Organizational
    GL_ACCOUNT VARCHAR(50),
    SUB_GL_ACCOUNT VARCHAR(50),
    PROFIT_CENTER VARCHAR(50),
    SALES_ORG VARCHAR(50),
    
    -- Payment Terms
    PAYMENT_TERMS VARCHAR(50),
    PAYMENT_TERMS_NAME VARCHAR(100),
    PAYMENT_TRANSACTION VARCHAR(50),
    
    -- Document Information
    ACCOUNTING_DOC_TYPE VARCHAR(10),
    ACCOUNTING_DOC_TYPE_NAME VARCHAR(100),
    POSTING_KEY VARCHAR(10),
    POSTING_KEY_NAME VARCHAR(100),
    
    -- References
    REF_DOC_NUM VARCHAR(50),
    REF_TRANSACTION VARCHAR(50),
    RESIDUAL_DOC VARCHAR(50),
    
    -- Other Attributes
    ACCOUNT_TYPE VARCHAR(1),
    SPECIAL_GL_INDICATOR VARCHAR(1),
    BALANCE_TYPE VARCHAR(50),
    ASSIGNMENT VARCHAR(50),
    REASON_CODE VARCHAR(10),
    
    -- Analyst
    CREDIT_ANALYST_NAME VARCHAR(100),
    CREDIT_ANALYST_ID VARCHAR(50),
    
    -- Source Organization
    SOURCE_ORG VARCHAR(50),
    
    -- Metadata
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (SOURCE_SYSTEM, COMPANY_CODE, POSTING_DATE)
COMMENT = 'Accounts receivable fact table from all source systems';

-- Create DIM_CUSTOMER (simplified)
CREATE OR REPLACE TABLE EDW.CORP_MASTER.DIM_CUSTOMER (
    -- Primary Key
    CUSTOMER_NUM_SK VARCHAR(50) NOT NULL,
    SOURCE_SYSTEM VARCHAR(50) NOT NULL,
    
    -- Customer Information
    CUSTOMER_NAME VARCHAR(500),
    CUSTOMER_TYPE VARCHAR(10),
    CUSTOMER_TYPE_NAME VARCHAR(50),
    CUSTOMER_CLASSIFICATION_NAME VARCHAR(100),
    CUSTOMER_ACCOUNT_GROUP VARCHAR(50),
    
    -- Location
    CUSTOMER_COUNTRY VARCHAR(10),
    CUSTOMER_COUNTRY_NAME VARCHAR(100),
    
    -- MDM Fields
    MDM_CUSTOMER_DUNS_NUM VARCHAR(50),
    MDM_CUSTOMER_FULL_NAME VARCHAR(500),
    MDM_CUSTOMER_GLOBAL_ULTIMATE_DUNS VARCHAR(50),
    MDM_CUSTOMER_GLOBAL_ULTIMATE_NAME VARCHAR(500),
    MDM_CUSTOMER_GLOBAL_ULTIMATE_PARENT_NAME VARCHAR(500),
    ULTIMATE_PARENT_SOURCE VARCHAR(50),
    
    -- Metadata
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (CUSTOMER_NUM_SK, SOURCE_SYSTEM)
)
COMMENT = 'Customer master dimension';

-- Create DIM_ENTITY (simplified)
CREATE OR REPLACE TABLE EDW.CORP_MASTER.DIM_ENTITY (
    -- Primary Key
    SOURCE_ENTITY_CODE_SK VARCHAR(50) NOT NULL,
    SOURCE_SYSTEM VARCHAR(50) NOT NULL,
    
    -- Entity Information
    ENTITY_NAME VARCHAR(200),
    ENTITY_COUNTRY_NAME VARCHAR(100),
    ENTITY_GLOBAL_REGION VARCHAR(50),
    ENTITY_GLOBAL_SUB_REGION VARCHAR(50),
    ENTITY_GLOBAL_SUB_REGION_NAME VARCHAR(100),
    ENTITY_REGION_CATEGORY VARCHAR(50),
    ENTITY_REGION_SUB_CATEGORY VARCHAR(50),
    ENTITY_STATUS VARCHAR(50),
    
    -- Metadata
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (SOURCE_ENTITY_CODE_SK, SOURCE_SYSTEM)
)
COMMENT = 'Legal entity master dimension';

-- Create TIME_FISCAL_DAY (reference calendar)
CREATE OR REPLACE TABLE EDW.CORP_REF.TIME_FISCAL_DAY (
    -- Primary Key
    FISCAL_DAY_KEY_STR VARCHAR(8) NOT NULL PRIMARY KEY,
    
    -- Date Fields
    FISCAL_DATE_KEY_DATE DATE NOT NULL,
    
    -- Fiscal Period Fields
    FISCAL_YEAR_STR VARCHAR(4),
    FISCAL_YEAR_INT NUMBER(4,0),
    FISCAL_PERIOD_STR VARCHAR(2),
    FISCAL_PERIOD_INT NUMBER(2,0),
    FISCAL_YEAR_PERIOD_STR VARCHAR(7),
    FISCAL_YEAR_PERIOD_INT NUMBER(6,0),
    FISCAL_YEAR_QUARTER_STR VARCHAR(6),
    
    -- Calendar Fields
    CALENDAR_YEAR NUMBER(4,0),
    CALENDAR_MONTH NUMBER(2,0),
    CALENDAR_DAY NUMBER(2,0),
    DAY_OF_WEEK VARCHAR(10),
    
    -- Metadata
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Fiscal calendar dimension';

-- ============================================================================
-- STEP 3: Populate Sample Data
-- ============================================================================

-- Populate TIME_FISCAL_DAY with 2 years of data
INSERT INTO EDW.CORP_REF.TIME_FISCAL_DAY
WITH date_range AS (
    SELECT 
        DATEADD(DAY, SEQ4(), '2023-01-01') AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))  -- 2 years
)
SELECT
    TO_CHAR(calendar_date, 'YYYYMMDD') AS FISCAL_DAY_KEY_STR,
    calendar_date AS FISCAL_DATE_KEY_DATE,
    TO_CHAR(calendar_date, 'YYYY') AS FISCAL_YEAR_STR,
    YEAR(calendar_date) AS FISCAL_YEAR_INT,
    LPAD(MONTH(calendar_date), 2, '0') AS FISCAL_PERIOD_STR,
    MONTH(calendar_date) AS FISCAL_PERIOD_INT,
    TO_CHAR(calendar_date, 'YYYY.MM') AS FISCAL_YEAR_PERIOD_STR,
    YEAR(calendar_date) * 100 + MONTH(calendar_date) AS FISCAL_YEAR_PERIOD_INT,
    TO_CHAR(calendar_date, 'YYYY.Q') || QUARTER(calendar_date) AS FISCAL_YEAR_QUARTER_STR,
    YEAR(calendar_date) AS CALENDAR_YEAR,
    MONTH(calendar_date) AS CALENDAR_MONTH,
    DAY(calendar_date) AS CALENDAR_DAY,
    DAYNAME(calendar_date) AS DAY_OF_WEEK,
    CURRENT_TIMESTAMP() AS LOAD_TS
FROM date_range;

-- Populate DIM_CUSTOMER with sample data
INSERT INTO EDW.CORP_MASTER.DIM_CUSTOMER VALUES
-- BRP900 Customers
('0001223776', 'BRP900', 'ABC Manufacturing Inc', 'E', 'EXTERNAL', 'Standard Customer', 'Z001', 'US', 'United States', '123456789', 'ABC Manufacturing Incorporated', '987654321', 'ABC Global Corp', 'ABC Global Corp', 'DNB', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('0001854954', 'BRP900', 'Honeywell Internal Entity', 'I', 'INTERNAL', 'Internal', 'Z099', 'US', 'United States', NULL, 'Honeywell International Inc', NULL, 'Honeywell International Inc', 'Honeywell International Inc', 'INTERNAL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('0001111111', 'BRP900', 'XYZ Corporation', 'E', 'EXTERNAL', 'Premium Customer', 'Z001', 'GB', 'United Kingdom', '111222333', 'XYZ Corporation Ltd', '999888777', 'XYZ Holdings', 'XYZ Holdings', 'DNB', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- CIP900 Customers
('0000001766', 'CIP900', 'Honeywell Trading Co', 'I', 'INTERNAL', 'Internal', 'Z099', 'DE', 'Germany', NULL, 'Honeywell Trading GmbH', NULL, 'Honeywell International Inc', 'Honeywell International Inc', 'INTERNAL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('0001245211', 'CIP900', 'Global Tech Solutions', 'E', 'EXTERNAL', 'Standard Customer', 'Z001', 'FR', 'France', '555666777', 'Global Tech Solutions SAS', '444333222', 'Global Tech Group', 'Global Tech Group', 'DNB', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- CIP300 Customers
('0000109845', 'CIP300', 'Honeywell Asia Pacific', 'I', 'INTERNAL', 'Internal', 'Z099', 'SG', 'Singapore', NULL, 'Honeywell Singapore Pte Ltd', NULL, 'Honeywell International Inc', 'Honeywell International Inc', 'INTERNAL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('0002222222', 'CIP300', 'Asia Manufacturing Ltd', 'E', 'EXTERNAL', 'Standard Customer', 'Z001', 'CN', 'China', '777888999', 'Asia Manufacturing Limited', '666555444', 'Asia Holdings', 'Asia Holdings', 'DNB', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Populate DIM_ENTITY with sample data
INSERT INTO EDW.CORP_MASTER.DIM_ENTITY VALUES
-- BRP900 Entities
('1000', 'BRP900', 'Honeywell Inc USA', 'United States', 'AMER', 'NA', 'North America', 'Developed Markets', 'US East', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('1100', 'BRP900', 'Honeywell UK Ltd', 'United Kingdom', 'EMEA', 'WE', 'Western Europe', 'Developed Markets', 'UK', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- CIP900 Entities
('2000', 'CIP900', 'Honeywell GmbH', 'Germany', 'EMEA', 'WE', 'Western Europe', 'Developed Markets', 'DACH', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('2100', 'CIP900', 'Honeywell France SAS', 'France', 'EMEA', 'WE', 'Western Europe', 'Developed Markets', 'France', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- CIP300 Entities
('3000', 'CIP300', 'Honeywell Singapore', 'Singapore', 'APAC', 'SEA', 'Southeast Asia', 'Emerging Markets', 'Singapore', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('3100', 'CIP300', 'Honeywell China Ltd', 'China', 'APAC', 'GC', 'Greater China', 'Emerging Markets', 'China East', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Populate FACT_ACCOUNT_RECEIVABLE_GBL with sample AR data
-- This creates realistic AR scenarios across different systems, aging buckets, and customer types

-- BRP900 Data
INSERT INTO EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL 
SELECT
    'BRP900' AS SOURCE_SYSTEM,
    '1000' AS COMPANY_CODE,
    'AR' || LPAD(SEQ4(), 10, '0') AS ACCOUNTING_DOC,
    '001' AS ACCOUNT_DOC_LINE_ITEM,
    '2024' AS FISCAL_YEAR,
    UNIFORM(1000, 100000, RANDOM()) AS AMT_DOC,
    UNIFORM(1000, 100000, RANDOM()) AS AMT_USD,
    UNIFORM(1000, 100000, RANDOM()) AS AMT_USD_ME,
    UNIFORM(1000, 100000, RANDOM()) AS AMT_LCL,
    'USD' AS DOC_CURR,
    'USD' AS LCL_CURR,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DOC_DATE,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS POSTING_DATE,
    DATEADD(DAY, -UNIFORM(0, 180, RANDOM()), CURRENT_DATE()) AS NET_DUE_DATE,
    NULL AS BASELINE_DATE,
    NULL AS CLEARING_DATE,  -- NULL = open item
    CASE WHEN UNIFORM(0, 100, RANDOM()) > 70 
         THEN '0001854954'  -- 30% internal
         ELSE '0001223776'  -- 70% external
    END AS SOLD_TO,
    SOLD_TO AS CUSTOMER_NUM_SK,
    '1300000' AS GL_ACCOUNT,
    NULL AS SUB_GL_ACCOUNT,
    'P1000' AS PROFIT_CENTER,
    'S001' AS SALES_ORG,
    'Z030' AS PAYMENT_TERMS,
    'Net 30 Days' AS PAYMENT_TERMS_NAME,
    NULL AS PAYMENT_TRANSACTION,
    'DR' AS ACCOUNTING_DOC_TYPE,
    'Customer Invoice' AS ACCOUNTING_DOC_TYPE_NAME,
    '01' AS POSTING_KEY,
    'Debit Customer' AS POSTING_KEY_NAME,
    NULL AS REF_DOC_NUM,
    NULL AS REF_TRANSACTION,
    NULL AS RESIDUAL_DOC,
    'D' AS ACCOUNT_TYPE,
    NULL AS SPECIAL_GL_INDICATOR,
    'Debit' AS BALANCE_TYPE,
    NULL AS ASSIGNMENT,
    NULL AS REASON_CODE,
    'John Smith' AS CREDIT_ANALYST_NAME,
    'JSMITH' AS CREDIT_ANALYST_ID,
    'P1000' AS SOURCE_ORG,
    CURRENT_TIMESTAMP() AS LOAD_TS,
    CURRENT_TIMESTAMP() AS UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 100));  -- 100 BRP900 invoices

-- CIP900 Data
INSERT INTO EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL 
SELECT
    'CIP900' AS SOURCE_SYSTEM,
    '2000' AS COMPANY_CODE,
    'CI' || LPAD(SEQ4(), 10, '0') AS ACCOUNTING_DOC,
    '001' AS ACCOUNT_DOC_LINE_ITEM,
    '2024' AS FISCAL_YEAR,
    UNIFORM(5000, 150000, RANDOM()) AS AMT_DOC,
    UNIFORM(5000, 150000, RANDOM()) AS AMT_USD,
    UNIFORM(5000, 150000, RANDOM()) AS AMT_USD_ME,
    UNIFORM(5000, 150000, RANDOM()) AS AMT_LCL,
    'EUR' AS DOC_CURR,
    'EUR' AS LCL_CURR,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DOC_DATE,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS POSTING_DATE,
    DATEADD(DAY, -UNIFORM(0, 200, RANDOM()), CURRENT_DATE()) AS NET_DUE_DATE,
    NULL AS BASELINE_DATE,
    NULL AS CLEARING_DATE,
    CASE WHEN UNIFORM(0, 100, RANDOM()) > 80 
         THEN '0000001766'  -- 20% internal
         ELSE '0001245211'  -- 80% external
    END AS SOLD_TO,
    SOLD_TO AS CUSTOMER_NUM_SK,
    '1300100' AS GL_ACCOUNT,
    NULL AS SUB_GL_ACCOUNT,
    'P2000' AS PROFIT_CENTER,
    'S002' AS SALES_ORG,
    'Z045' AS PAYMENT_TERMS,
    'Net 45 Days' AS PAYMENT_TERMS_NAME,
    NULL AS PAYMENT_TRANSACTION,
    'DR' AS ACCOUNTING_DOC_TYPE,
    'Customer Invoice' AS ACCOUNTING_DOC_TYPE_NAME,
    '01' AS POSTING_KEY,
    'Debit Customer' AS POSTING_KEY_NAME,
    NULL AS REF_DOC_NUM,
    NULL AS REF_TRANSACTION,
    NULL AS RESIDUAL_DOC,
    'D' AS ACCOUNT_TYPE,
    NULL AS SPECIAL_GL_INDICATOR,
    'Debit' AS BALANCE_TYPE,
    NULL AS ASSIGNMENT,
    NULL AS REASON_CODE,
    'Marie Dubois' AS CREDIT_ANALYST_NAME,
    'MDUBOIS' AS CREDIT_ANALYST_ID,
    'P2000' AS SOURCE_ORG,
    CURRENT_TIMESTAMP() AS LOAD_TS,
    CURRENT_TIMESTAMP() AS UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 80));  -- 80 CIP900 invoices

-- CIP300 Data  
INSERT INTO EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL 
SELECT
    'CIP300' AS SOURCE_SYSTEM,
    '3000' AS COMPANY_CODE,
    'AP' || LPAD(SEQ4(), 10, '0') AS ACCOUNTING_DOC,
    '001' AS ACCOUNT_DOC_LINE_ITEM,
    '2024' AS FISCAL_YEAR,
    UNIFORM(2000, 80000, RANDOM()) AS AMT_DOC,
    UNIFORM(2000, 80000, RANDOM()) AS AMT_USD,
    UNIFORM(2000, 80000, RANDOM()) AS AMT_USD_ME,
    UNIFORM(2000, 80000, RANDOM()) AS AMT_LCL,
    'SGD' AS DOC_CURR,
    'SGD' AS LCL_CURR,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DOC_DATE,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS POSTING_DATE,
    DATEADD(DAY, -UNIFORM(0, 150, RANDOM()), CURRENT_DATE()) AS NET_DUE_DATE,
    NULL AS BASELINE_DATE,
    NULL AS CLEARING_DATE,
    CASE WHEN UNIFORM(0, 100, RANDOM()) > 85 
         THEN '0000109845'  -- 15% internal
         ELSE '0002222222'  -- 85% external
    END AS SOLD_TO,
    SOLD_TO AS CUSTOMER_NUM_SK,
    '1300200' AS GL_ACCOUNT,
    NULL AS SUB_GL_ACCOUNT,
    'P3000' AS PROFIT_CENTER,
    'S003' AS SALES_ORG,
    'Z060' AS PAYMENT_TERMS,
    'Net 60 Days' AS PAYMENT_TERMS_NAME,
    NULL AS PAYMENT_TRANSACTION,
    'DR' AS ACCOUNTING_DOC_TYPE,
    'Customer Invoice' AS ACCOUNTING_DOC_TYPE_NAME,
    '01' AS POSTING_KEY,
    'Debit Customer' AS POSTING_KEY_NAME,
    NULL AS REF_DOC_NUM,
    NULL AS REF_TRANSACTION,
    NULL AS RESIDUAL_DOC,
    'D' AS ACCOUNT_TYPE,
    NULL AS SPECIAL_GL_INDICATOR,
    'Debit' AS BALANCE_TYPE,
    NULL AS ASSIGNMENT,
    NULL AS REASON_CODE,
    'Li Wei' AS CREDIT_ANALYST_NAME,
    'LWEI' AS CREDIT_ANALYST_ID,
    'P3000' AS SOURCE_ORG,
    CURRENT_TIMESTAMP() AS LOAD_TS,
    CURRENT_TIMESTAMP() AS UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 70));  -- 70 CIP300 invoices

-- ============================================================================
-- STEP 4: Verify Data Load
-- ============================================================================

-- Check record counts
SELECT 'FACT_ACCOUNT_RECEIVABLE_GBL' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
UNION ALL
SELECT 'DIM_CUSTOMER' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM EDW.CORP_MASTER.DIM_CUSTOMER
UNION ALL
SELECT 'DIM_ENTITY' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM EDW.CORP_MASTER.DIM_ENTITY
UNION ALL
SELECT 'TIME_FISCAL_DAY' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM EDW.CORP_REF.TIME_FISCAL_DAY;

-- Check AR data by source system
SELECT 
    SOURCE_SYSTEM,
    COUNT(*) AS INVOICE_COUNT,
    SUM(AMT_USD_ME) AS TOTAL_AMOUNT_USD,
    MIN(POSTING_DATE) AS EARLIEST_DATE,
    MAX(POSTING_DATE) AS LATEST_DATE
FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
GROUP BY SOURCE_SYSTEM
ORDER BY SOURCE_SYSTEM;

-- Check aging distribution
SELECT 
    SOURCE_SYSTEM,
    CASE 
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) <= 0 THEN 'CURRENT'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 1 AND 30 THEN '1-30'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 31 AND 60 THEN '31-60'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 61 AND 90 THEN '61-90'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) > 90 THEN '90+'
    END AS AGING_BUCKET,
    COUNT(*) AS INVOICE_COUNT,
    SUM(AMT_USD_ME) AS TOTAL_AMOUNT
FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
GROUP BY SOURCE_SYSTEM, AGING_BUCKET
ORDER BY SOURCE_SYSTEM, AGING_BUCKET;

-- ============================================================================
-- STEP 5: Grant Permissions
-- ============================================================================

-- Grant read access to raw data
GRANT USAGE ON DATABASE EDW TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_TRAN TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_MASTER TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_REF TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_REF TO ROLE DBT_ROLE;

-- Grant write access to DBT schemas
GRANT USAGE ON SCHEMA EDW.DEV_DBT TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA EDW.DBT_STAGING TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA EDW.DBT_SHARED TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_DM_FIN TO ROLE DBT_ROLE;

GRANT CREATE TABLE ON SCHEMA EDW.DEV_DBT TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA EDW.DBT_STAGING TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA EDW.DBT_SHARED TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA EDW.CORP_DM_FIN TO ROLE DBT_ROLE;

GRANT CREATE VIEW ON SCHEMA EDW.DEV_DBT TO ROLE DBT_ROLE;
GRANT CREATE VIEW ON SCHEMA EDW.DBT_STAGING TO ROLE DBT_ROLE;
GRANT CREATE VIEW ON SCHEMA EDW.DBT_SHARED TO ROLE DBT_ROLE;
GRANT CREATE VIEW ON SCHEMA EDW.CORP_DM_FIN TO ROLE DBT_ROLE;

-- ============================================================================
-- Data Preparation Complete
-- ============================================================================
-- Summary:
-- - Created 4 source tables (FACT_AR, DIM_CUSTOMER, DIM_ENTITY, TIME_FISCAL_DAY)
-- - Loaded 250 AR invoices across 3 systems (BRP900, CIP900, CIP300)
-- - Loaded 7 customers (mix of internal/external)
-- - Loaded 6 legal entities
-- - Loaded 730 days of fiscal calendar (2 years)
-- 
-- Next Steps:
-- 1. Run dbt_foundation project: dbt run --project dbt_foundation
-- 2. Run dbt_finance_core project: dbt run --project dbt_finance_core
-- 3. Query results: SELECT * FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE
-- ============================================================================

