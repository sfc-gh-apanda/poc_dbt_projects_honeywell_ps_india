-- ============================================================================
-- LOAD SAMPLE SOURCE DATA FOR DBT PROJECTS - PRODUCTION SCHEMA STRUCTURE
-- ============================================================================
-- Purpose: Create and populate source tables matching production schema
-- Idempotent: YES - uses CREATE OR REPLACE
-- Use Case: Development, testing, demos, monitoring data generation
-- Schema: Matches production (CORP_TRAN, CORP_MASTER, CORP_REF)
-- ============================================================================

-- WHAT THIS SCRIPT DOES:
-- 1. Creates 3 source schemas (CORP_TRAN, CORP_MASTER, CORP_REF)
-- 2. Loads 100 sample customers
-- 3. Loads 8 sample entities
-- 4. Loads 730 days of fiscal calendar (2024-2025)
-- 5. Loads 500 sample AR invoices with realistic aging buckets
-- 6. Grants permissions to dbt role
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ============================================================================
-- STEP 1: CREATE SOURCE SCHEMAS
-- ============================================================================

-- Transaction fact tables
CREATE SCHEMA IF NOT EXISTS CORP_TRAN 
    COMMENT = 'Transaction fact tables from source systems';

-- Master data dimensions
CREATE SCHEMA IF NOT EXISTS CORP_MASTER 
    COMMENT = 'Master dimension tables';

-- Reference and lookup tables
CREATE SCHEMA IF NOT EXISTS CORP_REF 
    COMMENT = 'Reference and lookup tables';

SELECT 'STEP 1 COMPLETE: Schemas created' as status;

-- ============================================================================
-- STEP 2: CREATE AND LOAD FISCAL CALENDAR DATA
-- ============================================================================

USE SCHEMA CORP_REF;

CREATE OR REPLACE TABLE TIME_FISCAL_DAY (
    FISCAL_DAY_KEY_STR VARCHAR(8) NOT NULL,
    FISCAL_DATE_KEY_DATE DATE NOT NULL,
    FISCAL_YEAR_STR VARCHAR(4),
    FISCAL_YEAR_INT NUMBER(4),
    FISCAL_PERIOD_STR VARCHAR(2),
    FISCAL_PERIOD_INT NUMBER(2),
    FISCAL_YEAR_PERIOD_STR VARCHAR(7),
    FISCAL_YEAR_PERIOD_INT NUMBER(6),
    FISCAL_YEAR_QUARTER_STR VARCHAR(6),
    CALENDAR_YEAR NUMBER(4),
    CALENDAR_MONTH NUMBER(2),
    CALENDAR_DAY NUMBER(2),
    DAY_OF_WEEK VARCHAR(10),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FISCAL_DAY_KEY_STR)
) COMMENT = 'Fiscal calendar dimension';

-- Insert 2 years of fiscal calendar data (2024-2025)
INSERT INTO TIME_FISCAL_DAY
WITH date_series AS (
    SELECT 
        DATEADD(day, seq, '2024-01-01'::DATE) as fiscal_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730)) seq
)
SELECT 
    TO_CHAR(fiscal_date, 'YYYYMMDD') as FISCAL_DAY_KEY_STR,
    fiscal_date as FISCAL_DATE_KEY_DATE,
    TO_CHAR(fiscal_date, 'YYYY') as FISCAL_YEAR_STR,
    YEAR(fiscal_date) as FISCAL_YEAR_INT,
    LPAD(MONTH(fiscal_date), 2, '0') as FISCAL_PERIOD_STR,
    MONTH(fiscal_date) as FISCAL_PERIOD_INT,
    TO_CHAR(fiscal_date, 'YYYY') || '.' || LPAD(MONTH(fiscal_date), 2, '0') as FISCAL_YEAR_PERIOD_STR,
    (YEAR(fiscal_date) * 100) + MONTH(fiscal_date) as FISCAL_YEAR_PERIOD_INT,
    TO_CHAR(fiscal_date, 'YYYY') || '.Q' || QUARTER(fiscal_date) as FISCAL_YEAR_QUARTER_STR,
    YEAR(fiscal_date) as CALENDAR_YEAR,
    MONTH(fiscal_date) as CALENDAR_MONTH,
    DAY(fiscal_date) as CALENDAR_DAY,
    DAYNAME(fiscal_date) as DAY_OF_WEEK,
    CURRENT_TIMESTAMP() as LOAD_TS
FROM date_series;

SELECT 'STEP 2 COMPLETE: Loaded ' || COUNT(*) || ' calendar days' as status
FROM TIME_FISCAL_DAY;

-- ============================================================================
-- STEP 3: CREATE AND LOAD CUSTOMER MASTER DATA
-- ============================================================================

USE SCHEMA CORP_MASTER;

CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_NUM_SK VARCHAR(50) NOT NULL,
    SOURCE_SYSTEM VARCHAR(50) NOT NULL,
    CUSTOMER_NAME VARCHAR(500),
    CUSTOMER_TYPE VARCHAR(10),
    CUSTOMER_TYPE_NAME VARCHAR(50),
    CUSTOMER_CLASSIFICATION_NAME VARCHAR(100),
    CUSTOMER_ACCOUNT_GROUP VARCHAR(50),
    CUSTOMER_COUNTRY VARCHAR(10),
    CUSTOMER_COUNTRY_NAME VARCHAR(100),
    MDM_CUSTOMER_DUNS_NUM VARCHAR(50),
    MDM_CUSTOMER_FULL_NAME VARCHAR(500),
    MDM_CUSTOMER_GLOBAL_ULTIMATE_DUNS VARCHAR(50),
    MDM_CUSTOMER_GLOBAL_ULTIMATE_NAME VARCHAR(500),
    MDM_CUSTOMER_GLOBAL_ULTIMATE_PARENT_NAME VARCHAR(500),
    ULTIMATE_PARENT_SOURCE VARCHAR(50),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CUSTOMER_NUM_SK, SOURCE_SYSTEM)
) COMMENT = 'Customer master dimension';

-- Insert 100 sample customers (mix of BRP900, CIP900, CIP300)
INSERT INTO DIM_CUSTOMER
SELECT 
    LPAD(seq, 10, '0') as CUSTOMER_NUM_SK,
    CASE 
        WHEN seq <= 40 THEN 'BRP900'
        WHEN seq <= 75 THEN 'CIP900'
        ELSE 'CIP300'
    END as SOURCE_SYSTEM,
    CASE 
        WHEN UNIFORM(1, 10, RANDOM()) <= 2 
        THEN 'Honeywell ' || CASE UNIFORM(1, 5, RANDOM())
            WHEN 1 THEN 'Internal Entity'
            WHEN 2 THEN 'Trading Co'
            WHEN 3 THEN 'Asia Pacific'
            WHEN 4 THEN 'Europe GmbH'
            ELSE 'Americas Inc'
        END
        ELSE 'Customer ' || seq || ' ' || CASE UNIFORM(1, 5, RANDOM())
            WHEN 1 THEN 'Inc'
            WHEN 2 THEN 'Ltd'
            WHEN 3 THEN 'Corp'
            WHEN 4 THEN 'GmbH'
            ELSE 'SAS'
        END
    END as CUSTOMER_NAME,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 8 THEN 'E' ELSE 'I' END as CUSTOMER_TYPE,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 8 THEN 'External' ELSE 'Internal' END as CUSTOMER_TYPE_NAME,
    CASE UNIFORM(1, 3, RANDOM())
        WHEN 1 THEN 'Standard Customer'
        WHEN 2 THEN 'Premium Customer'
        ELSE 'VIP Customer'
    END as CUSTOMER_CLASSIFICATION_NAME,
    'Z' || LPAD(UNIFORM(1, 999, RANDOM()), 3, '0') as CUSTOMER_ACCOUNT_GROUP,
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN 'US'
        WHEN 2 THEN 'GB'
        WHEN 3 THEN 'DE'
        WHEN 4 THEN 'FR'
        WHEN 5 THEN 'CN'
        WHEN 6 THEN 'SG'
        WHEN 7 THEN 'CA'
        ELSE 'JP'
    END as CUSTOMER_COUNTRY,
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN 'United States'
        WHEN 2 THEN 'United Kingdom'
        WHEN 3 THEN 'Germany'
        WHEN 4 THEN 'France'
        WHEN 5 THEN 'China'
        WHEN 6 THEN 'Singapore'
        WHEN 7 THEN 'Canada'
        ELSE 'Japan'
    END as CUSTOMER_COUNTRY_NAME,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 8 
         THEN LPAD(UNIFORM(100000000, 999999999, RANDOM()), 9, '0')
         ELSE NULL 
    END as MDM_CUSTOMER_DUNS_NUM,
    CUSTOMER_NAME || ' Full Legal Name' as MDM_CUSTOMER_FULL_NAME,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 8 
         THEN LPAD(UNIFORM(100000000, 999999999, RANDOM()), 9, '0')
         ELSE NULL 
    END as MDM_CUSTOMER_GLOBAL_ULTIMATE_DUNS,
    'Ultimate Parent ' || UNIFORM(1, 30, RANDOM()) as MDM_CUSTOMER_GLOBAL_ULTIMATE_NAME,
    'Ultimate Parent ' || UNIFORM(1, 30, RANDOM()) || ' Corporation' as MDM_CUSTOMER_GLOBAL_ULTIMATE_PARENT_NAME,
    CASE WHEN MDM_CUSTOMER_DUNS_NUM IS NOT NULL THEN 'DNB' ELSE 'INTERNAL' END as ULTIMATE_PARENT_SOURCE,
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_TIMESTAMP()) as LOAD_TS,
    DATEADD(day, -UNIFORM(1, 60, RANDOM()), CURRENT_TIMESTAMP()) as UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 100)) seq;

SELECT 'STEP 3 COMPLETE: Loaded ' || COUNT(*) || ' customers' as status
FROM DIM_CUSTOMER;

-- ============================================================================
-- STEP 4: CREATE AND LOAD ENTITY MASTER DATA
-- ============================================================================

CREATE OR REPLACE TABLE DIM_ENTITY (
    SOURCE_ENTITY_CODE_SK VARCHAR(50) NOT NULL,
    SOURCE_SYSTEM VARCHAR(50) NOT NULL,
    ENTITY_NAME VARCHAR(200),
    ENTITY_COUNTRY_NAME VARCHAR(100),
    ENTITY_GLOBAL_REGION VARCHAR(50),
    ENTITY_GLOBAL_SUB_REGION VARCHAR(50),
    ENTITY_GLOBAL_SUB_REGION_NAME VARCHAR(100),
    ENTITY_REGION_CATEGORY VARCHAR(50),
    ENTITY_REGION_SUB_CATEGORY VARCHAR(50),
    ENTITY_STATUS VARCHAR(50),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SOURCE_ENTITY_CODE_SK, SOURCE_SYSTEM)
) COMMENT = 'Legal entity master dimension';

-- Insert sample entities
INSERT INTO DIM_ENTITY VALUES
-- BRP900 Entities (US-based)
('1000', 'BRP900', 'Honeywell Inc USA', 'United States', 'AMER', 'NA', 'North America', 'Developed Markets', 'US East', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('1100', 'BRP900', 'Honeywell Canada Ltd', 'Canada', 'AMER', 'NA', 'North America', 'Developed Markets', 'Canada', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- CIP900 Entities (Europe)
('2000', 'CIP900', 'Honeywell GmbH', 'Germany', 'EMEA', 'WE', 'Western Europe', 'Developed Markets', 'DACH', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('2100', 'CIP900', 'Honeywell France SAS', 'France', 'EMEA', 'WE', 'Western Europe', 'Developed Markets', 'France', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('2200', 'CIP900', 'Honeywell UK Ltd', 'United Kingdom', 'EMEA', 'WE', 'Western Europe', 'Developed Markets', 'UK', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- CIP300 Entities (Asia Pacific)
('3000', 'CIP300', 'Honeywell Singapore Pte Ltd', 'Singapore', 'APAC', 'SEA', 'Southeast Asia', 'Emerging Markets', 'Singapore', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('3100', 'CIP300', 'Honeywell China Ltd', 'China', 'APAC', 'GC', 'Greater China', 'Emerging Markets', 'China East', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('3200', 'CIP300', 'Honeywell Japan KK', 'Japan', 'APAC', 'JP', 'Japan', 'Developed Markets', 'Japan', 'Active', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

SELECT 'STEP 4 COMPLETE: Loaded ' || COUNT(*) || ' entities' as status
FROM DIM_ENTITY;

-- ============================================================================
-- STEP 5: CREATE AND LOAD AR TRANSACTION DATA
-- ============================================================================

USE SCHEMA CORP_TRAN;

CREATE OR REPLACE TABLE FACT_ACCOUNT_RECEIVABLE_GBL (
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

-- Insert 500 sample AR invoices distributed across systems
-- BRP900 invoices (200 records)
INSERT INTO FACT_ACCOUNT_RECEIVABLE_GBL 
SELECT
    'BRP900' AS SOURCE_SYSTEM,
    CASE UNIFORM(1, 2, RANDOM()) WHEN 1 THEN '1000' ELSE '1100' END AS COMPANY_CODE,
    'AR' || LPAD(seq, 10, '0') AS ACCOUNTING_DOC,
    '001' AS ACCOUNT_DOC_LINE_ITEM,
    '2024' AS FISCAL_YEAR,
    ROUND(UNIFORM(1000, 100000, RANDOM()), 2) AS AMT_DOC,
    ROUND(UNIFORM(1000, 100000, RANDOM()), 2) AS AMT_USD,
    ROUND(UNIFORM(1000, 100000, RANDOM()), 2) AS AMT_USD_ME,
    ROUND(UNIFORM(1000, 100000, RANDOM()), 2) AS AMT_LCL,
    'USD' AS DOC_CURR,
    'USD' AS LCL_CURR,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DOC_DATE,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS POSTING_DATE,
    DATEADD(DAY, -UNIFORM(0, 180, RANDOM()), CURRENT_DATE()) AS NET_DUE_DATE,
    NULL AS BASELINE_DATE,
    NULL AS CLEARING_DATE,  -- NULL = open item
    LPAD(UNIFORM(1, 40, RANDOM()), 10, '0') AS SOLD_TO,
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
    DATEADD(hour, -UNIFORM(1, 48, RANDOM()), CURRENT_TIMESTAMP()) AS LOAD_TS,
    DATEADD(hour, -UNIFORM(1, 24, RANDOM()), CURRENT_TIMESTAMP()) AS UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 200)) seq;

-- CIP900 invoices (200 records)
INSERT INTO FACT_ACCOUNT_RECEIVABLE_GBL 
SELECT
    'CIP900' AS SOURCE_SYSTEM,
    CASE UNIFORM(1, 3, RANDOM()) 
        WHEN 1 THEN '2000'
        WHEN 2 THEN '2100'
        ELSE '2200'
    END AS COMPANY_CODE,
    'CI' || LPAD(seq, 10, '0') AS ACCOUNTING_DOC,
    '001' AS ACCOUNT_DOC_LINE_ITEM,
    '2024' AS FISCAL_YEAR,
    ROUND(UNIFORM(5000, 150000, RANDOM()), 2) AS AMT_DOC,
    ROUND(UNIFORM(5000, 150000, RANDOM()), 2) * 1.1 AS AMT_USD,  -- EUR to USD conversion
    ROUND(UNIFORM(5000, 150000, RANDOM()), 2) * 1.1 AS AMT_USD_ME,
    ROUND(UNIFORM(5000, 150000, RANDOM()), 2) AS AMT_LCL,
    'EUR' AS DOC_CURR,
    'EUR' AS LCL_CURR,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DOC_DATE,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS POSTING_DATE,
    DATEADD(DAY, -UNIFORM(0, 200, RANDOM()), CURRENT_DATE()) AS NET_DUE_DATE,
    NULL AS BASELINE_DATE,
    NULL AS CLEARING_DATE,
    LPAD(UNIFORM(41, 75, RANDOM()), 10, '0') AS SOLD_TO,
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
    DATEADD(hour, -UNIFORM(1, 48, RANDOM()), CURRENT_TIMESTAMP()) AS LOAD_TS,
    DATEADD(hour, -UNIFORM(1, 24, RANDOM()), CURRENT_TIMESTAMP()) AS UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 200)) seq;

-- CIP300 invoices (100 records)
INSERT INTO FACT_ACCOUNT_RECEIVABLE_GBL 
SELECT
    'CIP300' AS SOURCE_SYSTEM,
    CASE UNIFORM(1, 3, RANDOM())
        WHEN 1 THEN '3000'
        WHEN 2 THEN '3100'
        ELSE '3200'
    END AS COMPANY_CODE,
    'AP' || LPAD(seq, 10, '0') AS ACCOUNTING_DOC,
    '001' AS ACCOUNT_DOC_LINE_ITEM,
    '2024' AS FISCAL_YEAR,
    ROUND(UNIFORM(2000, 80000, RANDOM()), 2) AS AMT_DOC,
    ROUND(UNIFORM(2000, 80000, RANDOM()), 2) * 0.75 AS AMT_USD,  -- SGD/CNY to USD conversion
    ROUND(UNIFORM(2000, 80000, RANDOM()), 2) * 0.75 AS AMT_USD_ME,
    ROUND(UNIFORM(2000, 80000, RANDOM()), 2) AS AMT_LCL,
    CASE UNIFORM(1, 2, RANDOM()) WHEN 1 THEN 'SGD' ELSE 'CNY' END AS DOC_CURR,
    DOC_CURR AS LCL_CURR,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DOC_DATE,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS POSTING_DATE,
    DATEADD(DAY, -UNIFORM(0, 150, RANDOM()), CURRENT_DATE()) AS NET_DUE_DATE,
    NULL AS BASELINE_DATE,
    NULL AS CLEARING_DATE,
    LPAD(UNIFORM(76, 100, RANDOM()), 10, '0') AS SOLD_TO,
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
    DATEADD(hour, -UNIFORM(1, 48, RANDOM()), CURRENT_TIMESTAMP()) AS LOAD_TS,
    DATEADD(hour, -UNIFORM(1, 24, RANDOM()), CURRENT_TIMESTAMP()) AS UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 100)) seq;

SELECT 'STEP 5 COMPLETE: Loaded ' || COUNT(*) || ' AR invoices' as status
FROM FACT_ACCOUNT_RECEIVABLE_GBL;

-- ============================================================================
-- STEP 6: GRANT PERMISSIONS
-- ============================================================================

-- Grant read access to source schemas
GRANT USAGE ON SCHEMA EDW.CORP_TRAN TO ROLE DBT_DEV_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_MASTER TO ROLE DBT_DEV_ROLE;
GRANT USAGE ON SCHEMA EDW.CORP_REF TO ROLE DBT_DEV_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_REF TO ROLE DBT_DEV_ROLE;

GRANT SELECT ON FUTURE TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA EDW.CORP_REF TO ROLE DBT_DEV_ROLE;

SELECT 'STEP 6 COMPLETE: Permissions granted to DBT_DEV_ROLE' as status;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'VERIFICATION: Data Load Summary' as check_type;

SELECT 
    'FACT_ACCOUNT_RECEIVABLE_GBL' as table_name,
    'EDW.CORP_TRAN' as schema_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT SOURCE_SYSTEM) as distinct_systems,
    COUNT(DISTINCT COMPANY_CODE) as distinct_companies
FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL

UNION ALL

SELECT 
    'DIM_CUSTOMER',
    'EDW.CORP_MASTER',
    COUNT(*),
    COUNT(DISTINCT SOURCE_SYSTEM),
    COUNT(DISTINCT CUSTOMER_COUNTRY)
FROM EDW.CORP_MASTER.DIM_CUSTOMER

UNION ALL

SELECT 
    'DIM_ENTITY',
    'EDW.CORP_MASTER',
    COUNT(*),
    COUNT(DISTINCT SOURCE_SYSTEM),
    COUNT(DISTINCT ENTITY_COUNTRY_NAME)
FROM EDW.CORP_MASTER.DIM_ENTITY

UNION ALL

SELECT 
    'TIME_FISCAL_DAY',
    'EDW.CORP_REF',
    COUNT(*),
    COUNT(DISTINCT FISCAL_YEAR_INT),
    COUNT(DISTINCT FISCAL_PERIOD_INT)
FROM EDW.CORP_REF.TIME_FISCAL_DAY;

-- Check AR data by source system
SELECT 'VERIFICATION: AR Data Distribution' as check_type;

SELECT 
    SOURCE_SYSTEM,
    COMPANY_CODE,
    COUNT(*) AS invoice_count,
    ROUND(SUM(AMT_USD_ME), 2) AS total_amount_usd,
    MIN(POSTING_DATE) AS earliest_date,
    MAX(POSTING_DATE) AS latest_date
FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
GROUP BY SOURCE_SYSTEM, COMPANY_CODE
ORDER BY SOURCE_SYSTEM, COMPANY_CODE;

-- Check AR aging distribution
SELECT 'VERIFICATION: AR Aging Distribution' as check_type;

SELECT 
    SOURCE_SYSTEM,
    CASE 
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) < 0 THEN 'Not Due'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 0 AND 30 THEN '0-30 Days'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 31 AND 60 THEN '31-60 Days'
        WHEN DATEDIFF(DAY, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 61 AND 90 THEN '61-90 Days'
        ELSE '90+ Days'
    END as aging_bucket,
    COUNT(*) as invoice_count,
    ROUND(SUM(AMT_USD_ME), 2) as total_amount_usd,
    ROUND(AVG(AMT_USD_ME), 2) as avg_amount_usd
FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL
GROUP BY SOURCE_SYSTEM, aging_bucket
ORDER BY SOURCE_SYSTEM, 
    CASE aging_bucket
        WHEN 'Not Due' THEN 1
        WHEN '0-30 Days' THEN 2
        WHEN '31-60 Days' THEN 3
        WHEN '61-90 Days' THEN 4
        ELSE 5
    END;

-- Preview sample data
SELECT 'VERIFICATION: Sample AR Records' as check_type;
SELECT * FROM EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL LIMIT 5;

SELECT 'VERIFICATION: Sample Customer Records' as check_type;
SELECT * FROM EDW.CORP_MASTER.DIM_CUSTOMER LIMIT 5;

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================

SELECT '
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║        ✅ SAMPLE SOURCE DATA LOADED - PRODUCTION SCHEMA STRUCTURE!       ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

📊 DATA LOADED:

✅ Schema: EDW.CORP_TRAN
   └─ FACT_ACCOUNT_RECEIVABLE_GBL: 500 records
      - BRP900: 200 invoices (US/Canada)
      - CIP900: 200 invoices (Europe)
      - CIP300: 100 invoices (Asia Pacific)

✅ Schema: EDW.CORP_MASTER
   ├─ DIM_CUSTOMER: 100 records
   │  - BRP900: 40 customers
   │  - CIP900: 35 customers
   │  - CIP300: 25 customers
   │  - Mix of External/Internal customers
   │
   └─ DIM_ENTITY: 8 records
      - BRP900: 2 entities (Americas)
      - CIP900: 3 entities (Europe)
      - CIP300: 3 entities (Asia Pacific)

✅ Schema: EDW.CORP_REF
   └─ TIME_FISCAL_DAY: 730 records
      - Date range: 2024-2025 (2 years)
      - All fiscal periods (1-12)

✅ Permissions granted to DBT_DEV_ROLE

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 NEXT STEPS:

1. Run dbt_foundation:
   - Navigate to dbt_foundation project in Snowsight
   - Click "Build" or run models
   - Creates: stg_ar_invoice, dim_customer, dim_fiscal_calendar

2. Run dbt_finance_core:
   - Navigate to dbt_finance_core project in Snowsight
   - Click "Build" or run models  
   - Creates: dm_fin_ar_aging_simple, dm_fin_ar_aging_simple_v2

3. Check results:
   SELECT * FROM EDW.DEV_DBT_DBT_SHARED.DIM_CUSTOMER;
   SELECT * FROM EDW.DEV_DBT_DBT_SHARED.DIM_FISCAL_CALENDAR;
   SELECT * FROM EDW.DEV_DBT_DBT_STAGING.STG_AR_INVOICE;
   SELECT * FROM EDW.DEV_DBT_DBT_FINANCE.DM_FIN_AR_AGING_SIMPLE;

4. Set up monitoring:
   - Run MASTER_SETUP_QUERY_HISTORY.sql
   - Query History will automatically capture your dbt runs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ MATCHES PRODUCTION SCHEMA STRUCTURE - NO DBT CHANGES NEEDED!

🎉 READY TO RUN DBT PROJECTS!

' as setup_complete;
