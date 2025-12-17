-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - SAMPLE TEST DATA SETUP
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Create and populate source tables for testing all 5 data load patterns
-- 
-- Tables Created:
--   EDW.CORP_MASTER.DIM_CUSTOMER       - Customer master data
--   EDW.CORP_MASTER.DIM_PAYMENT_TERMS  - Payment terms reference
--   EDW.CORP_MASTER.DIM_BANK_ACCOUNT   - Bank account master
--   EDW.CORP_TRAN.FACT_SALES_ORDERS    - Sales orders (multi-source)
--   EDW.CORP_TRAN.FACT_INVOICES        - Customer invoices
--   EDW.CORP_TRAN.FACT_PAYMENTS        - Payment transactions
-- 
-- Source Systems Included:
--   - BRP (North America ERP)
--   - CIP (Asia Pacific ERP)
--   - SAP (Europe ERP)
-- 
-- Prerequisites:
--   - Database EDW must exist
--   - Run with ACCOUNTADMIN or equivalent
-- 
-- Idempotent: YES - Safe to run multiple times (uses CREATE OR REPLACE)
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: CREATE SOURCE SCHEMAS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS CORP_TRAN
    COMMENT = 'Corporate transactional data - Orders, Invoices, Payments';

CREATE SCHEMA IF NOT EXISTS CORP_MASTER
    COMMENT = 'Corporate master data - Customers, Payment Terms, Banks';

SELECT '✅ STEP 1 COMPLETE: Source schemas created' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: CREATE MASTER DATA TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- DIM_CUSTOMER - Customer Master Data
-- ───────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE CORP_MASTER.DIM_CUSTOMER (
    CUSTOMER_NUM_SK   VARCHAR(50)   COMMENT 'Customer surrogate key',
    CUSTOMER_NAME     VARCHAR(200)  COMMENT 'Customer legal name',
    SOURCE_SYSTEM     VARCHAR(20)   COMMENT 'Source ERP system (BRP, CIP, SAP)',
    CUSTOMER_COUNTRY  VARCHAR(100)  COMMENT 'Customer country',
    CUSTOMER_SEGMENT  VARCHAR(50)   COMMENT 'Customer segment (Enterprise, Mid-Market, SMB)',
    CREDIT_LIMIT      NUMBER(18,2)  COMMENT 'Credit limit in local currency',
    PAYMENT_TERMS     VARCHAR(20)   COMMENT 'Default payment terms code',
    IS_ACTIVE         BOOLEAN       DEFAULT TRUE COMMENT 'Active customer flag',
    LOAD_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp'
);

COMMENT ON TABLE CORP_MASTER.DIM_CUSTOMER IS 
    'Customer master data from multiple ERP systems';

-- Insert sample customers across different sources/regions
INSERT INTO CORP_MASTER.DIM_CUSTOMER 
    (CUSTOMER_NUM_SK, CUSTOMER_NAME, SOURCE_SYSTEM, CUSTOMER_COUNTRY, CUSTOMER_SEGMENT, CREDIT_LIMIT, PAYMENT_TERMS)
VALUES
    -- BRP (North America)
    ('CUST-BRP-001', 'Acme Corporation', 'BRP', 'USA', 'Enterprise', 500000, 'NET30'),
    ('CUST-BRP-002', 'GlobalTech Industries', 'BRP', 'USA', 'Enterprise', 750000, 'NET45'),
    ('CUST-BRP-003', 'Maple Leaf Manufacturing', 'BRP', 'Canada', 'Mid-Market', 250000, 'NET30'),
    ('CUST-BRP-004', 'Southwest Distributors', 'BRP', 'Mexico', 'SMB', 100000, 'NET15'),
    
    -- CIP (Asia Pacific)
    ('CUST-CIP-001', 'Tokyo Electronics Ltd', 'CIP', 'Japan', 'Enterprise', 900000, 'NET60'),
    ('CUST-CIP-002', 'Pacific Trading Co', 'CIP', 'Australia', 'Mid-Market', 350000, 'NET30'),
    ('CUST-CIP-003', 'Seoul Tech Industries', 'CIP', 'South Korea', 'Enterprise', 600000, 'NET45'),
    ('CUST-CIP-004', 'Singapore Solutions Pte', 'CIP', 'Singapore', 'SMB', 150000, 'NET15'),
    
    -- SAP (Europe)
    ('CUST-SAP-001', 'EuroMfg GmbH', 'SAP', 'Germany', 'Enterprise', 800000, 'NET60'),
    ('CUST-SAP-002', 'Paris Industrielle SA', 'SAP', 'France', 'Enterprise', 650000, 'NET45'),
    ('CUST-SAP-003', 'Nordic Components AB', 'SAP', 'Sweden', 'Mid-Market', 300000, 'NET30'),
    ('CUST-SAP-004', 'Italia Manufacturing SpA', 'SAP', 'Italy', 'Mid-Market', 400000, 'NET30');

-- ───────────────────────────────────────────────────────────────────────────────
-- DIM_PAYMENT_TERMS - Payment Terms Reference
-- ───────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE CORP_MASTER.DIM_PAYMENT_TERMS (
    PAYMENT_TERMS_CODE VARCHAR(20)  COMMENT 'Payment terms code',
    PAYMENT_TERMS_NAME VARCHAR(100) COMMENT 'Payment terms description',
    DAYS_TO_PAY        INTEGER      COMMENT 'Number of days until payment due',
    DISCOUNT_PERCENT   NUMBER(5,2)  DEFAULT 0 COMMENT 'Early payment discount %',
    DISCOUNT_DAYS      INTEGER      DEFAULT 0 COMMENT 'Days to qualify for discount',
    SOURCE_SYSTEM      VARCHAR(20)  COMMENT 'Source system',
    IS_ACTIVE          BOOLEAN      DEFAULT TRUE COMMENT 'Active flag',
    LOAD_TS            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp'
);

COMMENT ON TABLE CORP_MASTER.DIM_PAYMENT_TERMS IS 
    'Payment terms reference data';

INSERT INTO CORP_MASTER.DIM_PAYMENT_TERMS 
    (PAYMENT_TERMS_CODE, PAYMENT_TERMS_NAME, DAYS_TO_PAY, DISCOUNT_PERCENT, DISCOUNT_DAYS, SOURCE_SYSTEM)
VALUES
    ('NET15', 'Net 15 Days', 15, 0, 0, 'BRP'),
    ('NET30', 'Net 30 Days', 30, 0, 0, 'BRP'),
    ('NET45', 'Net 45 Days', 45, 0, 0, 'CIP'),
    ('NET60', 'Net 60 Days', 60, 0, 0, 'SAP'),
    ('2/10NET30', '2% 10 Net 30', 30, 2.00, 10, 'BRP'),
    ('1/15NET45', '1% 15 Net 45', 45, 1.00, 15, 'CIP');

-- ───────────────────────────────────────────────────────────────────────────────
-- DIM_BANK_ACCOUNT - Bank Account Master
-- ───────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE CORP_MASTER.DIM_BANK_ACCOUNT (
    BANK_ACCOUNT_ID VARCHAR(50)  COMMENT 'Bank account identifier',
    BANK_NAME       VARCHAR(200) COMMENT 'Bank name',
    ACCOUNT_TYPE    VARCHAR(50)  COMMENT 'Account type (CHECKING, SAVINGS)',
    CURRENCY_CODE   VARCHAR(3)   COMMENT 'Account currency',
    SOURCE_SYSTEM   VARCHAR(20)  COMMENT 'Source system',
    IS_ACTIVE       BOOLEAN      DEFAULT TRUE COMMENT 'Active flag',
    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp'
);

COMMENT ON TABLE CORP_MASTER.DIM_BANK_ACCOUNT IS 
    'Bank account master data for payment processing';

INSERT INTO CORP_MASTER.DIM_BANK_ACCOUNT 
    (BANK_ACCOUNT_ID, BANK_NAME, ACCOUNT_TYPE, CURRENCY_CODE, SOURCE_SYSTEM)
VALUES
    ('BA-BRP-001', 'Chase Bank NA', 'CHECKING', 'USD', 'BRP'),
    ('BA-BRP-002', 'Bank of America', 'CHECKING', 'USD', 'BRP'),
    ('BA-CIP-001', 'HSBC Hong Kong', 'CHECKING', 'HKD', 'CIP'),
    ('BA-CIP-002', 'Mitsubishi UFJ', 'CHECKING', 'JPY', 'CIP'),
    ('BA-SAP-001', 'Deutsche Bank', 'CHECKING', 'EUR', 'SAP'),
    ('BA-SAP-002', 'BNP Paribas', 'CHECKING', 'EUR', 'SAP');

SELECT '✅ STEP 2 COMPLETE: Master data tables created and populated' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: CREATE TRANSACTIONAL DATA TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────────────────────
-- FACT_SALES_ORDERS - Sales Orders (Multi-Source)
-- ───────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE CORP_TRAN.FACT_SALES_ORDERS (
    ORDER_ID        VARCHAR(50)   COMMENT 'Order identifier',
    ORDER_LINE      INTEGER       COMMENT 'Order line number',
    SOURCE_SYSTEM   VARCHAR(20)   COMMENT 'Source ERP system (BRP, CIP, SAP)',
    CUSTOMER_ID     VARCHAR(50)   COMMENT 'Customer identifier',
    ORDER_DATE      DATE          COMMENT 'Order creation date',
    ORDER_AMOUNT    NUMBER(18,2)  COMMENT 'Order line amount',
    ORDER_CURRENCY  VARCHAR(3)    COMMENT 'Order currency code',
    ORDER_STATUS    VARCHAR(20)   COMMENT 'Order status (OPEN, SHIPPED, COMPLETED, CANCELLED)',
    SHIP_DATE       DATE          COMMENT 'Actual ship date',
    CREATED_DATE    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
);

COMMENT ON TABLE CORP_TRAN.FACT_SALES_ORDERS IS 
    'Sales orders from multiple ERP systems - primary O2C source';

-- Generate orders across all three source systems for last 15 days
-- This creates varied data for testing different patterns
INSERT INTO CORP_TRAN.FACT_SALES_ORDERS 
    (ORDER_ID, ORDER_LINE, SOURCE_SYSTEM, CUSTOMER_ID, ORDER_DATE, ORDER_AMOUNT, ORDER_CURRENCY, ORDER_STATUS, SHIP_DATE)

-- BRP Orders (North America) - 40 orders
SELECT 
    'ORD-BRP-' || LPAD(seq.n::VARCHAR, 5, '0') AS ORDER_ID,
    1 AS ORDER_LINE,
    'BRP' AS SOURCE_SYSTEM,
    CASE MOD(seq.n, 4) 
        WHEN 0 THEN 'CUST-BRP-001'
        WHEN 1 THEN 'CUST-BRP-002'
        WHEN 2 THEN 'CUST-BRP-003'
        ELSE 'CUST-BRP-004'
    END AS CUSTOMER_ID,
    DATEADD('day', -MOD(seq.n, 15), CURRENT_DATE()) AS ORDER_DATE,
    ROUND(UNIFORM(5000, 75000, RANDOM()), 2) AS ORDER_AMOUNT,
    'USD' AS ORDER_CURRENCY,
    CASE 
        WHEN MOD(seq.n, 15) > 10 THEN 'OPEN'
        WHEN MOD(seq.n, 15) > 5 THEN 'SHIPPED'
        WHEN MOD(seq.n, 10) = 0 THEN 'CANCELLED'
        ELSE 'COMPLETED'
    END AS ORDER_STATUS,
    CASE 
        WHEN MOD(seq.n, 15) <= 10 THEN DATEADD('day', -MOD(seq.n, 15) + 2, CURRENT_DATE())
        ELSE NULL
    END AS SHIP_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 40)) AS gen
CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n FROM TABLE(GENERATOR(ROWCOUNT => 40))) AS seq

UNION ALL

-- CIP Orders (Asia Pacific) - 35 orders
SELECT 
    'ORD-CIP-' || LPAD((seq.n + 1000)::VARCHAR, 5, '0') AS ORDER_ID,
    1 AS ORDER_LINE,
    'CIP' AS SOURCE_SYSTEM,
    CASE MOD(seq.n, 4) 
        WHEN 0 THEN 'CUST-CIP-001'
        WHEN 1 THEN 'CUST-CIP-002'
        WHEN 2 THEN 'CUST-CIP-003'
        ELSE 'CUST-CIP-004'
    END AS CUSTOMER_ID,
    DATEADD('day', -MOD(seq.n, 15), CURRENT_DATE()) AS ORDER_DATE,
    ROUND(UNIFORM(10000, 120000, RANDOM()), 2) AS ORDER_AMOUNT,
    CASE MOD(seq.n, 3) WHEN 0 THEN 'JPY' WHEN 1 THEN 'AUD' ELSE 'SGD' END AS ORDER_CURRENCY,
    CASE 
        WHEN MOD(seq.n, 15) > 10 THEN 'OPEN'
        WHEN MOD(seq.n, 15) > 5 THEN 'SHIPPED'
        WHEN MOD(seq.n, 12) = 0 THEN 'CANCELLED'
        ELSE 'COMPLETED'
    END AS ORDER_STATUS,
    CASE 
        WHEN MOD(seq.n, 15) <= 10 THEN DATEADD('day', -MOD(seq.n, 15) + 3, CURRENT_DATE())
        ELSE NULL
    END AS SHIP_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 35)) AS gen
CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n FROM TABLE(GENERATOR(ROWCOUNT => 35))) AS seq

UNION ALL

-- SAP Orders (Europe) - 30 orders
SELECT 
    'ORD-SAP-' || LPAD((seq.n + 2000)::VARCHAR, 5, '0') AS ORDER_ID,
    1 AS ORDER_LINE,
    'SAP' AS SOURCE_SYSTEM,
    CASE MOD(seq.n, 4) 
        WHEN 0 THEN 'CUST-SAP-001'
        WHEN 1 THEN 'CUST-SAP-002'
        WHEN 2 THEN 'CUST-SAP-003'
        ELSE 'CUST-SAP-004'
    END AS CUSTOMER_ID,
    DATEADD('day', -MOD(seq.n, 15), CURRENT_DATE()) AS ORDER_DATE,
    ROUND(UNIFORM(8000, 100000, RANDOM()), 2) AS ORDER_AMOUNT,
    'EUR' AS ORDER_CURRENCY,
    CASE 
        WHEN MOD(seq.n, 15) > 10 THEN 'OPEN'
        WHEN MOD(seq.n, 15) > 5 THEN 'SHIPPED'
        WHEN MOD(seq.n, 8) = 0 THEN 'CANCELLED'
        ELSE 'COMPLETED'
    END AS ORDER_STATUS,
    CASE 
        WHEN MOD(seq.n, 15) <= 10 THEN DATEADD('day', -MOD(seq.n, 15) + 2, CURRENT_DATE())
        ELSE NULL
    END AS SHIP_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 30)) AS gen
CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n FROM TABLE(GENERATOR(ROWCOUNT => 30))) AS seq;

-- ───────────────────────────────────────────────────────────────────────────────
-- FACT_INVOICES - Customer Invoices
-- ───────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE CORP_TRAN.FACT_INVOICES (
    INVOICE_ID      VARCHAR(50)   COMMENT 'Invoice identifier',
    INVOICE_LINE    INTEGER       COMMENT 'Invoice line number',
    SOURCE_SYSTEM   VARCHAR(20)   COMMENT 'Source ERP system',
    ORDER_ID        VARCHAR(50)   COMMENT 'Related order ID',
    ORDER_LINE      INTEGER       COMMENT 'Related order line',
    INVOICE_DATE    DATE          COMMENT 'Invoice date',
    INVOICE_AMOUNT  NUMBER(18,2)  COMMENT 'Invoice amount',
    INVOICE_CURRENCY VARCHAR(3)   COMMENT 'Invoice currency',
    PAYMENT_TERMS   VARCHAR(20)   COMMENT 'Payment terms code',
    DUE_DATE        DATE          COMMENT 'Payment due date',
    INVOICE_STATUS  VARCHAR(20)   COMMENT 'Invoice status (OPEN, PARTIAL, PAID, VOID)',
    CREATED_DATE    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
);

COMMENT ON TABLE CORP_TRAN.FACT_INVOICES IS 
    'Customer invoices linked to sales orders';

-- Create invoices for shipped and completed orders
INSERT INTO CORP_TRAN.FACT_INVOICES 
    (INVOICE_ID, INVOICE_LINE, SOURCE_SYSTEM, ORDER_ID, ORDER_LINE, INVOICE_DATE, INVOICE_AMOUNT, INVOICE_CURRENCY, PAYMENT_TERMS, DUE_DATE, INVOICE_STATUS)
SELECT 
    'INV-' || SUBSTR(ORDER_ID, 5) AS INVOICE_ID,
    1 AS INVOICE_LINE,
    SOURCE_SYSTEM,
    ORDER_ID,
    ORDER_LINE,
    DATEADD('day', 1, ORDER_DATE) AS INVOICE_DATE,
    ORDER_AMOUNT AS INVOICE_AMOUNT,
    ORDER_CURRENCY AS INVOICE_CURRENCY,
    CASE SOURCE_SYSTEM 
        WHEN 'BRP' THEN 'NET30'
        WHEN 'CIP' THEN 'NET45'
        ELSE 'NET60'
    END AS PAYMENT_TERMS,
    CASE SOURCE_SYSTEM 
        WHEN 'BRP' THEN DATEADD('day', 31, ORDER_DATE)
        WHEN 'CIP' THEN DATEADD('day', 46, ORDER_DATE)
        ELSE DATEADD('day', 61, ORDER_DATE)
    END AS DUE_DATE,
    CASE 
        WHEN ORDER_DATE < DATEADD('day', -10, CURRENT_DATE()) THEN 'PAID'
        WHEN ORDER_DATE < DATEADD('day', -5, CURRENT_DATE()) THEN 'PARTIAL'
        ELSE 'OPEN'
    END AS INVOICE_STATUS
FROM CORP_TRAN.FACT_SALES_ORDERS
WHERE ORDER_STATUS IN ('SHIPPED', 'COMPLETED');

-- ───────────────────────────────────────────────────────────────────────────────
-- FACT_PAYMENTS - Payment Transactions
-- ───────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE CORP_TRAN.FACT_PAYMENTS (
    PAYMENT_ID       VARCHAR(50)   COMMENT 'Payment identifier',
    SOURCE_SYSTEM    VARCHAR(20)   COMMENT 'Source ERP system',
    INVOICE_ID       VARCHAR(50)   COMMENT 'Related invoice ID',
    INVOICE_LINE     INTEGER       COMMENT 'Related invoice line',
    PAYMENT_DATE     DATE          COMMENT 'Payment date',
    PAYMENT_AMOUNT   NUMBER(18,2)  COMMENT 'Payment amount',
    PAYMENT_CURRENCY VARCHAR(3)    COMMENT 'Payment currency',
    PAYMENT_METHOD   VARCHAR(50)   COMMENT 'Payment method (WIRE, CHECK, ACH)',
    BANK_ACCOUNT_ID  VARCHAR(50)   COMMENT 'Bank account used',
    PAYMENT_STATUS   VARCHAR(20)   COMMENT 'Payment status (PENDING, CLEARED, REJECTED)',
    CREATED_DATE     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
);

COMMENT ON TABLE CORP_TRAN.FACT_PAYMENTS IS 
    'Payment transactions linked to invoices';

-- Create payments for PAID and PARTIAL invoices
INSERT INTO CORP_TRAN.FACT_PAYMENTS 
    (PAYMENT_ID, SOURCE_SYSTEM, INVOICE_ID, INVOICE_LINE, PAYMENT_DATE, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_METHOD, BANK_ACCOUNT_ID, PAYMENT_STATUS)
SELECT 
    'PAY-' || SUBSTR(INVOICE_ID, 5) AS PAYMENT_ID,
    SOURCE_SYSTEM,
    INVOICE_ID,
    1 AS INVOICE_LINE,
    DATEADD('day', 
        CASE SOURCE_SYSTEM WHEN 'BRP' THEN 25 WHEN 'CIP' THEN 40 ELSE 55 END,
        INVOICE_DATE
    ) AS PAYMENT_DATE,
    CASE INVOICE_STATUS
        WHEN 'PAID' THEN INVOICE_AMOUNT
        ELSE ROUND(INVOICE_AMOUNT * 0.5, 2)  -- Partial payment = 50%
    END AS PAYMENT_AMOUNT,
    INVOICE_CURRENCY AS PAYMENT_CURRENCY,
    CASE MOD(ABS(HASH(INVOICE_ID)), 3) 
        WHEN 0 THEN 'WIRE'
        WHEN 1 THEN 'ACH'
        ELSE 'CHECK'
    END AS PAYMENT_METHOD,
    CASE SOURCE_SYSTEM 
        WHEN 'BRP' THEN 'BA-BRP-001'
        WHEN 'CIP' THEN 'BA-CIP-001'
        ELSE 'BA-SAP-001'
    END AS BANK_ACCOUNT_ID,
    'CLEARED' AS PAYMENT_STATUS
FROM CORP_TRAN.FACT_INVOICES
WHERE INVOICE_STATUS IN ('PAID', 'PARTIAL');

SELECT '✅ STEP 3 COMPLETE: Transactional data tables created and populated' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: GRANT PERMISSIONS TO DBT ROLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Grant to dbt developer role
GRANT USAGE ON SCHEMA EDW.CORP_TRAN TO ROLE DBT_O2C_DEVELOPER;
GRANT USAGE ON SCHEMA EDW.CORP_MASTER TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE DBT_O2C_DEVELOPER;

-- Future grants
GRANT SELECT ON FUTURE TABLES IN SCHEMA EDW.CORP_TRAN TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA EDW.CORP_MASTER TO ROLE DBT_O2C_DEVELOPER;

SELECT '✅ STEP 4 COMPLETE: Permissions granted to DBT_O2C_DEVELOPER' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5: VERIFICATION & DATA SUMMARY
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ O2C ENHANCED SAMPLE DATA SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Summary: Row counts
SELECT 'MASTER DATA' AS category, 'DIM_CUSTOMER' AS table_name, COUNT(*) AS row_count FROM CORP_MASTER.DIM_CUSTOMER
UNION ALL SELECT 'MASTER DATA', 'DIM_PAYMENT_TERMS', COUNT(*) FROM CORP_MASTER.DIM_PAYMENT_TERMS
UNION ALL SELECT 'MASTER DATA', 'DIM_BANK_ACCOUNT', COUNT(*) FROM CORP_MASTER.DIM_BANK_ACCOUNT
UNION ALL SELECT 'TRANSACTIONAL', 'FACT_SALES_ORDERS', COUNT(*) FROM CORP_TRAN.FACT_SALES_ORDERS
UNION ALL SELECT 'TRANSACTIONAL', 'FACT_INVOICES', COUNT(*) FROM CORP_TRAN.FACT_INVOICES
UNION ALL SELECT 'TRANSACTIONAL', 'FACT_PAYMENTS', COUNT(*) FROM CORP_TRAN.FACT_PAYMENTS
ORDER BY category, table_name;

-- Summary: Orders by source system
SELECT '--- ORDERS BY SOURCE SYSTEM ---' AS report;
SELECT 
    SOURCE_SYSTEM,
    COUNT(*) AS order_count,
    SUM(ORDER_AMOUNT) AS total_amount,
    MIN(ORDER_DATE) AS earliest_order,
    MAX(ORDER_DATE) AS latest_order
FROM CORP_TRAN.FACT_SALES_ORDERS
GROUP BY SOURCE_SYSTEM
ORDER BY SOURCE_SYSTEM;

-- Summary: Orders by status
SELECT '--- ORDERS BY STATUS ---' AS report;
SELECT 
    ORDER_STATUS,
    COUNT(*) AS order_count
FROM CORP_TRAN.FACT_SALES_ORDERS
GROUP BY ORDER_STATUS
ORDER BY order_count DESC;

-- Summary: Orders by date (for delete+insert testing)
SELECT '--- ORDERS BY DATE (Last 10 Days) ---' AS report;
SELECT 
    ORDER_DATE,
    COUNT(*) AS order_count,
    COUNT(DISTINCT SOURCE_SYSTEM) AS source_systems
FROM CORP_TRAN.FACT_SALES_ORDERS
WHERE ORDER_DATE >= DATEADD('day', -10, CURRENT_DATE())
GROUP BY ORDER_DATE
ORDER BY ORDER_DATE DESC;

