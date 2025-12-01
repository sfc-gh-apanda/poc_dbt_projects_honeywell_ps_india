-- ============================================================================
-- LOAD SAMPLE SOURCE DATA FOR DBT PROJECTS
-- ============================================================================
-- Purpose: Create and populate source tables for dbt development and testing
-- Idempotent: YES - uses CREATE OR REPLACE
-- Use Case: Development, testing, demos, monitoring data generation
-- ============================================================================

-- WHAT THIS SCRIPT DOES:
-- 1. Creates CORP_REF schema for source tables
-- 2. Loads 100 sample customers
-- 3. Loads 730 days of fiscal calendar (2024-2025)
-- 4. Loads 500 sample AR invoices with realistic aging buckets
-- 5. Grants permissions to dbt role
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ============================================================================
-- STEP 1: CREATE SOURCE SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS CORP_REF
    COMMENT = 'Corporate reference data - source for dbt foundation layer';

USE SCHEMA CORP_REF;

SELECT 'STEP 1 COMPLETE: Schema created' as status;

-- ============================================================================
-- STEP 2: CREATE AND LOAD CUSTOMER DATA
-- ============================================================================

CREATE OR REPLACE TABLE CUSTOMER (
    CUSTOMER_NUM_SK VARCHAR(50),
    SOURCE_SYSTEM VARCHAR(20),
    CUSTOMER_NAME VARCHAR(255),
    CUSTOMER_TYPE VARCHAR(10),
    CUSTOMER_TYPE_NAME VARCHAR(100),
    CUSTOMER_CLASSIFICATION VARCHAR(100),
    CUSTOMER_ACCOUNT_GROUP VARCHAR(50),
    CUSTOMER_COUNTRY VARCHAR(5),
    CUSTOMER_COUNTRY_NAME VARCHAR(100),
    DUNS_NUMBER VARCHAR(50),
    MDM_CUSTOMER_FULL_NAME VARCHAR(255),
    GLOBAL_ULTIMATE_DUNS VARCHAR(50),
    GLOBAL_ULTIMATE_NAME VARCHAR(255),
    GLOBAL_ULTIMATE_PARENT_NAME VARCHAR(255),
    ULTIMATE_PARENT_SOURCE VARCHAR(50),
    LOAD_TS TIMESTAMP_NTZ,
    UPDATE_TS TIMESTAMP_NTZ
);

-- Insert 100 sample customers
INSERT INTO CUSTOMER
SELECT 
    'CUST' || LPAD(seq, 6, '0') as CUSTOMER_NUM_SK,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 7 THEN 'SAP_US' ELSE 'SAP_EU' END as SOURCE_SYSTEM,
    'Customer ' || seq || ' Inc.' as CUSTOMER_NAME,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 9 THEN 'E' ELSE 'I' END as CUSTOMER_TYPE,
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 9 THEN 'External' ELSE 'Internal' END as CUSTOMER_TYPE_NAME,
    'Standard Customer' as CUSTOMER_CLASSIFICATION,
    'ACC' || UNIFORM(100, 999, RANDOM()) as CUSTOMER_ACCOUNT_GROUP,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'US'
        WHEN 2 THEN 'CA'
        WHEN 3 THEN 'GB'
        WHEN 4 THEN 'DE'
        ELSE 'FR'
    END as CUSTOMER_COUNTRY,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'United States'
        WHEN 2 THEN 'Canada'
        WHEN 3 THEN 'United Kingdom'
        WHEN 4 THEN 'Germany'
        ELSE 'France'
    END as CUSTOMER_COUNTRY_NAME,
    LPAD(UNIFORM(100000000, 999999999, RANDOM()), 9, '0') as DUNS_NUMBER,
    'Customer ' || seq || ' Full Name' as MDM_CUSTOMER_FULL_NAME,
    LPAD(UNIFORM(100000000, 999999999, RANDOM()), 9, '0') as GLOBAL_ULTIMATE_DUNS,
    'Ultimate Parent ' || UNIFORM(1, 20, RANDOM()) as GLOBAL_ULTIMATE_NAME,
    'Ultimate Parent ' || UNIFORM(1, 20, RANDOM()) || ' Corp' as GLOBAL_ULTIMATE_PARENT_NAME,
    'MDM' as ULTIMATE_PARENT_SOURCE,
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_TIMESTAMP()) as LOAD_TS,
    DATEADD(day, -UNIFORM(1, 30, RANDOM()), CURRENT_TIMESTAMP()) as UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 100)) seq;

SELECT 'STEP 2 COMPLETE: Loaded ' || COUNT(*) || ' customers' as status
FROM CUSTOMER;

-- ============================================================================
-- STEP 3: CREATE AND LOAD FISCAL CALENDAR DATA
-- ============================================================================

CREATE OR REPLACE TABLE TIME_FISCAL_DAY (
    FISCAL_DAY_KEY_STR VARCHAR(10),
    FISCAL_DATE DATE,
    FISCAL_YEAR_STR VARCHAR(4),
    FISCAL_YEAR_INT NUMBER(4),
    FISCAL_PERIOD_STR VARCHAR(2),
    FISCAL_PERIOD_INT NUMBER(2),
    FISCAL_YEAR_PERIOD_STR VARCHAR(7),
    FISCAL_YEAR_PERIOD_INT NUMBER(6),
    FISCAL_YEAR_QUARTER_STR VARCHAR(7),
    CALENDAR_YEAR NUMBER(4),
    CALENDAR_MONTH NUMBER(2),
    CALENDAR_DAY NUMBER(2),
    DAY_OF_WEEK VARCHAR(20),
    LOAD_TS TIMESTAMP_NTZ
);

-- Insert 2 years of fiscal calendar data (2024-2025)
INSERT INTO TIME_FISCAL_DAY
WITH date_series AS (
    SELECT 
        DATEADD(day, seq, '2024-01-01'::DATE) as fiscal_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730)) seq
)
SELECT 
    TO_CHAR(fiscal_date, 'YYYYMMDD') as FISCAL_DAY_KEY_STR,
    fiscal_date as FISCAL_DATE,
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

SELECT 'STEP 3 COMPLETE: Loaded ' || COUNT(*) || ' calendar days' as status
FROM TIME_FISCAL_DAY;

-- ============================================================================
-- STEP 4: CREATE AND LOAD AR INVOICE DATA
-- ============================================================================

CREATE OR REPLACE TABLE AR_INVOICE_OPEN (
    SOURCE_SYSTEM VARCHAR(20),
    COMPANY_CODE VARCHAR(10),
    DOCUMENT_NUMBER VARCHAR(50),
    DOCUMENT_LINE NUMBER(5),
    DOCUMENT_YEAR NUMBER(4),
    CUSTOMER_NUMBER VARCHAR(50),
    AMT_USD_ME NUMBER(18,2),
    AMT_DOC NUMBER(18,2),
    AMT_LCL NUMBER(18,2),
    DOC_CURRENCY VARCHAR(5),
    LOCAL_CURRENCY VARCHAR(5),
    DOCUMENT_DATE DATE,
    POSTING_DATE DATE,
    NET_DUE_DATE DATE,
    BASELINE_DATE DATE,
    CLEARING_DATE DATE,
    GL_ACCOUNT VARCHAR(20),
    SUB_GL_ACCOUNT VARCHAR(20),
    PROFIT_CENTER VARCHAR(20),
    SALES_ORGANIZATION VARCHAR(10),
    PAYMENT_TERMS VARCHAR(20),
    PAYMENT_TERMS_NAME VARCHAR(100),
    PAYMENT_INDEX NUMBER(3),
    DOCUMENT_TYPE_SK VARCHAR(10),
    DOC_TYPE_DESC VARCHAR(100),
    POSTING_KEY VARCHAR(5),
    POSTING_KEY_NAME VARCHAR(100),
    REF_DOC_NUM VARCHAR(50),
    REFERENCE_TRANSACTION VARCHAR(50),
    INVOICE_REF VARCHAR(50),
    ACCOUNT_TYPE VARCHAR(5),
    SPECIAL_GL_INDICATOR VARCHAR(10),
    BALANCE_TYPE VARCHAR(10),
    ASSIGNMENT VARCHAR(50),
    REASON_CODE VARCHAR(20),
    CREDIT_ANALYST_NAME VARCHAR(100),
    CREDIT_ANALYST_ID VARCHAR(50),
    SOURCE_ORG VARCHAR(50),
    LOAD_TS TIMESTAMP_NTZ,
    UPDATE_TS TIMESTAMP_NTZ
);

-- Insert 500 sample AR invoices with realistic aging buckets
INSERT INTO AR_INVOICE_OPEN
SELECT 
    CASE WHEN UNIFORM(1, 10, RANDOM()) <= 7 THEN 'SAP_US' ELSE 'SAP_EU' END as SOURCE_SYSTEM,
    'C' || LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') as COMPANY_CODE,
    'INV' || LPAD(seq, 10, '0') as DOCUMENT_NUMBER,
    1 as DOCUMENT_LINE,
    2024 as DOCUMENT_YEAR,
    'CUST' || LPAD(UNIFORM(1, 100, RANDOM()), 6, '0') as CUSTOMER_NUMBER,
    ROUND(UNIFORM(100, 50000, RANDOM()), 2) as AMT_USD_ME,
    ROUND(UNIFORM(100, 50000, RANDOM()), 2) as AMT_DOC,
    ROUND(UNIFORM(100, 50000, RANDOM()), 2) as AMT_LCL,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'USD'
        WHEN 2 THEN 'EUR'
        WHEN 3 THEN 'GBP'
        ELSE 'CAD'
    END as DOC_CURRENCY,
    'USD' as LOCAL_CURRENCY,
    DATEADD(day, -UNIFORM(1, 180, RANDOM()), CURRENT_DATE()) as DOCUMENT_DATE,
    DATEADD(day, -UNIFORM(1, 180, RANDOM()), CURRENT_DATE()) as POSTING_DATE,
    DATEADD(day, UNIFORM(0, 90, RANDOM()), DATEADD(day, -UNIFORM(1, 180, RANDOM()), CURRENT_DATE())) as NET_DUE_DATE,
    DATEADD(day, -UNIFORM(1, 180, RANDOM()), CURRENT_DATE()) as BASELINE_DATE,
    NULL as CLEARING_DATE,  -- NULL for open items
    '1100' || UNIFORM(10, 99, RANDOM()) as GL_ACCOUNT,
    '11001' as SUB_GL_ACCOUNT,
    'PC' || UNIFORM(100, 999, RANDOM()) as PROFIT_CENTER,
    'SO' || UNIFORM(1000, 9999, RANDOM()) as SALES_ORGANIZATION,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'NET30'
        WHEN 2 THEN 'NET45'
        WHEN 3 THEN 'NET60'
        ELSE 'NET90'
    END as PAYMENT_TERMS,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'Net 30 Days'
        WHEN 2 THEN 'Net 45 Days'
        WHEN 3 THEN 'Net 60 Days'
        ELSE 'Net 90 Days'
    END as PAYMENT_TERMS_NAME,
    1 as PAYMENT_INDEX,
    'DR' as DOCUMENT_TYPE_SK,
    'Customer Invoice' as DOC_TYPE_DESC,
    '01' as POSTING_KEY,
    'Debit' as POSTING_KEY_NAME,
    'REF' || LPAD(seq, 8, '0') as REF_DOC_NUM,
    NULL as REFERENCE_TRANSACTION,
    NULL as INVOICE_REF,
    'D' as ACCOUNT_TYPE,  -- Debit for AR
    NULL as SPECIAL_GL_INDICATOR,
    'N' as BALANCE_TYPE,
    'ASG' || seq as ASSIGNMENT,
    NULL as REASON_CODE,
    'Analyst ' || UNIFORM(1, 10, RANDOM()) as CREDIT_ANALYST_NAME,
    'AN' || LPAD(UNIFORM(1, 10, RANDOM()), 3, '0') as CREDIT_ANALYST_ID,
    'US_SALES' as SOURCE_ORG,
    DATEADD(hour, -UNIFORM(1, 48, RANDOM()), CURRENT_TIMESTAMP()) as LOAD_TS,
    DATEADD(hour, -UNIFORM(1, 24, RANDOM()), CURRENT_TIMESTAMP()) as UPDATE_TS
FROM TABLE(GENERATOR(ROWCOUNT => 500)) seq;

SELECT 'STEP 4 COMPLETE: Loaded ' || COUNT(*) || ' AR invoices' as status
FROM AR_INVOICE_OPEN;

-- ============================================================================
-- STEP 5: GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA EDW.CORP_REF TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_REF TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA EDW.CORP_REF TO ROLE DBT_DEV_ROLE;

SELECT 'STEP 5 COMPLETE: Permissions granted to DBT_DEV_ROLE' as status;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'VERIFICATION: Data Load Summary' as check_type;

SELECT 
    'CUSTOMER' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT SOURCE_SYSTEM) as distinct_systems,
    COUNT(DISTINCT CUSTOMER_COUNTRY) as distinct_countries
FROM CUSTOMER

UNION ALL

SELECT 
    'TIME_FISCAL_DAY',
    COUNT(*),
    COUNT(DISTINCT FISCAL_YEAR_INT),
    COUNT(DISTINCT FISCAL_PERIOD_INT)
FROM TIME_FISCAL_DAY

UNION ALL

SELECT 
    'AR_INVOICE_OPEN',
    COUNT(*),
    COUNT(DISTINCT CUSTOMER_NUMBER),
    COUNT(DISTINCT COMPANY_CODE)
FROM AR_INVOICE_OPEN;

-- Preview the data
SELECT 'VERIFICATION: Sample Customer Data' as check_type;
SELECT * FROM CUSTOMER LIMIT 5;

SELECT 'VERIFICATION: Sample Fiscal Calendar Data' as check_type;
SELECT * FROM TIME_FISCAL_DAY ORDER BY FISCAL_DATE DESC LIMIT 5;

SELECT 'VERIFICATION: Sample AR Invoice Data' as check_type;
SELECT * FROM AR_INVOICE_OPEN LIMIT 5;

-- Check AR aging distribution
SELECT 'VERIFICATION: AR Aging Distribution' as check_type;
SELECT 
    CASE 
        WHEN DATEDIFF(day, NET_DUE_DATE, CURRENT_DATE()) < 0 THEN 'Not Due'
        WHEN DATEDIFF(day, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 0 AND 30 THEN '0-30 Days'
        WHEN DATEDIFF(day, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 31 AND 60 THEN '31-60 Days'
        WHEN DATEDIFF(day, NET_DUE_DATE, CURRENT_DATE()) BETWEEN 61 AND 90 THEN '61-90 Days'
        ELSE '90+ Days'
    END as aging_bucket,
    COUNT(*) as invoice_count,
    ROUND(SUM(AMT_USD_ME), 2) as total_amount_usd,
    ROUND(AVG(AMT_USD_ME), 2) as avg_amount_usd
FROM AR_INVOICE_OPEN
GROUP BY 1
ORDER BY 
    CASE aging_bucket
        WHEN 'Not Due' THEN 1
        WHEN '0-30 Days' THEN 2
        WHEN '31-60 Days' THEN 3
        WHEN '61-90 Days' THEN 4
        ELSE 5
    END;

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================

SELECT '
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║              ✅ SAMPLE SOURCE DATA LOADED SUCCESSFULLY!                  ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

📊 DATA LOADED:

✅ Schema: EDW.CORP_REF
✅ CUSTOMER: 100 records
   - 2 source systems (SAP_US, SAP_EU)
   - 5 countries
   - Mix of External/Internal customers

✅ TIME_FISCAL_DAY: 730 records
   - Date range: 2024-2025 (2 years)
   - All fiscal periods (1-12)

✅ AR_INVOICE_OPEN: 500 records
   - Distributed across aging buckets
   - Various payment terms
   - Open items only (NULL clearing_date)

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
   SELECT * FROM EDW.DEV_DBT.DIM_CUSTOMER;
   SELECT * FROM EDW.DEV_DBT.DIM_FISCAL_CALENDAR;
   SELECT * FROM EDW.DEV_DBT.DM_FIN_AR_AGING_SIMPLE;

4. Set up monitoring:
   - Run MASTER_SETUP_QUERY_HISTORY.sql (uses Query History, not dbt_artifacts)
   - Query History will automatically capture your dbt runs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎉 READY TO RUN DBT PROJECTS!

' as setup_complete;

