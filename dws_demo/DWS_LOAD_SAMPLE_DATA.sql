-- ═══════════════════════════════════════════════════════════════════════════════
-- DWS CLIENT REPORTING - SAMPLE DATA LOAD SCRIPT
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Load sample data for DWS Client Reporting dbt demo project
-- Domain:  Asset Management (DWS Group style)
-- Tables:  8 source tables across 3 schemas
-- Records: ~1,500 sample records
--
-- Usage:
--   snowsql -f DWS_LOAD_SAMPLE_DATA.sql
--   OR run directly in Snowsight
--
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SYSADMIN;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DATABASE & SCHEMA SETUP
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS DWS_EDW;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DBT DEPLOY PREREQUISITES (network access for dbt package installs)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS DWS_EDW.DBT_DEPLOY;
USE SCHEMA DWS_EDW.DBT_DEPLOY;

CREATE OR REPLACE NETWORK RULE dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com');

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_ext_access
  ALLOWED_NETWORK_RULES = (DWS_EDW.DBT_DEPLOY.dbt_network_rule)
  ENABLED = TRUE;

USE ROLE SYSADMIN;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SOURCE SCHEMAS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS DWS_EDW.DWS_TRAN;
CREATE SCHEMA IF NOT EXISTS DWS_EDW.DWS_MASTER;
CREATE SCHEMA IF NOT EXISTS DWS_EDW.DWS_REF;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 1: DIM_CLIENT (Master Data - 30 clients)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_MASTER.DIM_CLIENT (
    client_id           VARCHAR(20)     NOT NULL,
    client_name         VARCHAR(200)    NOT NULL,
    client_type         VARCHAR(20)     NOT NULL,   -- INSTITUTIONAL, RETAIL, PRIVATE_BANK
    client_segment      VARCHAR(50),                -- SOVEREIGN, PENSION, INSURANCE, HNWI, RETAIL
    domicile_country    VARCHAR(5),
    domicile_country_name VARCHAR(100),
    risk_profile        VARCHAR(20),                -- CONSERVATIVE, BALANCED, GROWTH, AGGRESSIVE
    relationship_manager VARCHAR(100),
    onboarding_date     DATE,
    tax_id              VARCHAR(50),
    lei_code            VARCHAR(20),                -- Legal Entity Identifier
    is_active           BOOLEAN         DEFAULT TRUE,
    load_ts             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    update_ts           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DWS_EDW.DWS_MASTER.DIM_CLIENT VALUES
    ('CLI001', 'Bayerische Versicherungskammer',   'INSTITUTIONAL', 'INSURANCE',  'DE', 'Germany',        'BALANCED',      'Hans Mueller',     '2018-03-15', 'DE123456789', 'LEI00000001', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI002', 'Nordrhein-Westfalen Pension Fund', 'INSTITUTIONAL', 'PENSION',    'DE', 'Germany',        'CONSERVATIVE',  'Hans Mueller',     '2017-06-01', 'DE234567890', 'LEI00000002', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI003', 'Swiss Re Investment Partners',     'INSTITUTIONAL', 'INSURANCE',  'CH', 'Switzerland',    'GROWTH',        'Anna Schmidt',     '2019-01-10', 'CH345678901', 'LEI00000003', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI004', 'Abu Dhabi Investment Authority',   'INSTITUTIONAL', 'SOVEREIGN',  'AE', 'UAE',            'AGGRESSIVE',    'Ahmed Al-Rashid',  '2016-09-20', 'AE456789012', 'LEI00000004', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI005', 'Caisse de Depot Quebec',           'INSTITUTIONAL', 'PENSION',    'CA', 'Canada',         'BALANCED',      'Pierre Dubois',    '2018-11-05', 'CA567890123', 'LEI00000005', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI006', 'UK Universities Superannuation',   'INSTITUTIONAL', 'PENSION',    'GB', 'United Kingdom', 'BALANCED',      'James Wilson',     '2017-02-28', 'GB678901234', 'LEI00000006', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI007', 'Generali Investment Holdings',     'INSTITUTIONAL', 'INSURANCE',  'IT', 'Italy',          'CONSERVATIVE',  'Marco Rossi',      '2019-04-15', 'IT789012345', 'LEI00000007', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI008', 'Japan Post Insurance',             'INSTITUTIONAL', 'INSURANCE',  'JP', 'Japan',          'CONSERVATIVE',  'Yuki Tanaka',      '2020-01-20', 'JP890123456', 'LEI00000008', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI009', 'CalPERS',                          'INSTITUTIONAL', 'PENSION',    'US', 'United States',  'GROWTH',        'Sarah Johnson',    '2015-07-10', 'US901234567', 'LEI00000009', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI010', 'Temasek Holdings',                 'INSTITUTIONAL', 'SOVEREIGN',  'SG', 'Singapore',      'AGGRESSIVE',    'Wei Lin Chen',     '2018-05-22', 'SG012345678', 'LEI00000010', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI011', 'Dr. Klaus-Peter von Harenberg',    'PRIVATE_BANK',  'HNWI',       'DE', 'Germany',        'GROWTH',        'Anna Schmidt',     '2020-03-01', 'DE111222333', NULL,          TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI012', 'Maria Fernandez Trust',            'PRIVATE_BANK',  'HNWI',       'ES', 'Spain',          'BALANCED',      'Marco Rossi',      '2021-06-15', 'ES222333444', NULL,          TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI013', 'Chen Family Office',               'PRIVATE_BANK',  'HNWI',       'HK', 'Hong Kong',      'AGGRESSIVE',    'Wei Lin Chen',     '2019-08-20', 'HK333444555', NULL,          TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI014', 'William & Elizabeth Foundation',   'INSTITUTIONAL', 'PENSION',    'GB', 'United Kingdom', 'CONSERVATIVE',  'James Wilson',     '2022-01-10', 'GB444555666', 'LEI00000014', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI015', 'Allianz SE Treasury',              'INSTITUTIONAL', 'INSURANCE',  'DE', 'Germany',        'BALANCED',      'Hans Mueller',     '2016-11-30', 'DE555666777', 'LEI00000015', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI016', 'Ontario Teachers Pension Plan',    'INSTITUTIONAL', 'PENSION',    'CA', 'Canada',         'GROWTH',        'Pierre Dubois',    '2017-04-20', 'CA666777888', 'LEI00000016', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI017', 'Norges Bank Investment Mgmt',      'INSTITUTIONAL', 'SOVEREIGN',  'NO', 'Norway',         'BALANCED',      'Anna Schmidt',     '2015-01-05', 'NO777888999', 'LEI00000017', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI018', 'GIC Private Limited',              'INSTITUTIONAL', 'SOVEREIGN',  'SG', 'Singapore',      'GROWTH',        'Wei Lin Chen',     '2016-08-15', 'SG888999000', 'LEI00000018', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI019', 'Retail Client - Thomas Braun',     'RETAIL',        'RETAIL',     'DE', 'Germany',        'CONSERVATIVE',  'Hans Mueller',     '2023-01-15', 'DE999000111', NULL,          TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI020', 'Retail Client - Sophie Laurent',   'RETAIL',        'RETAIL',     'FR', 'France',         'BALANCED',      'Pierre Dubois',    '2023-03-20', 'FR000111222', NULL,          TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI021', 'Kuwait Investment Authority',      'INSTITUTIONAL', 'SOVEREIGN',  'KW', 'Kuwait',         'AGGRESSIVE',    'Ahmed Al-Rashid',  '2017-09-01', 'KW111222333', 'LEI00000021', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI022', 'AP Pension Denmark',               'INSTITUTIONAL', 'PENSION',    'DK', 'Denmark',        'BALANCED',      'Anna Schmidt',     '2019-12-10', 'DK222333444', 'LEI00000022', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI023', 'Samsung Life Insurance',           'INSTITUTIONAL', 'INSURANCE',  'KR', 'South Korea',    'CONSERVATIVE',  'Yuki Tanaka',      '2020-05-25', 'KR333444555', 'LEI00000023', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI024', 'Retail Client - James Smith',      'RETAIL',        'RETAIL',     'GB', 'United Kingdom', 'GROWTH',        'James Wilson',     '2023-06-01', 'GB444555666', NULL,          TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI025', 'Mubadala Investment Company',      'INSTITUTIONAL', 'SOVEREIGN',  'AE', 'UAE',            'AGGRESSIVE',    'Ahmed Al-Rashid',  '2018-02-14', 'AE555666777', 'LEI00000025', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI026', 'Defunct Holdings GmbH',            'INSTITUTIONAL', 'INSURANCE',  'DE', 'Germany',        'CONSERVATIVE',  'Hans Mueller',     '2019-01-01', 'DE666777888', 'LEI00000026', FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI027', 'Closed Pension Fund UK',           'INSTITUTIONAL', 'PENSION',    'GB', 'United Kingdom', 'CONSERVATIVE',  'James Wilson',     '2018-06-01', 'GB777888999', 'LEI00000027', FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI028', 'BPIFrance Investissement',         'INSTITUTIONAL', 'SOVEREIGN',  'FR', 'France',         'BALANCED',      'Pierre Dubois',    '2020-10-01', 'FR888999000', 'LEI00000028', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI029', 'National Pension Service Korea',   'INSTITUTIONAL', 'PENSION',    'KR', 'South Korea',    'GROWTH',        'Yuki Tanaka',      '2017-03-15', 'KR999000111', 'LEI00000029', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('CLI030', 'Future Fund Australia',            'INSTITUTIONAL', 'SOVEREIGN',  'AU', 'Australia',      'GROWTH',        'Sarah Johnson',    '2019-07-22', 'AU000111222', 'LEI00000030', TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 2: DIM_ACCOUNT (Master Data - 40 accounts/mandates)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_MASTER.DIM_ACCOUNT (
    account_id          VARCHAR(20)     NOT NULL,
    client_id           VARCHAR(20)     NOT NULL,
    account_name        VARCHAR(200),
    account_type        VARCHAR(30),                -- SEGREGATED, POOLED, ADVISORY, DISCRETIONARY
    mandate_type        VARCHAR(50),                -- EQUITY, FIXED_INCOME, MULTI_ASSET, ALTERNATIVES
    base_currency       VARCHAR(3),
    inception_date      DATE,
    benchmark_id        VARCHAR(20),
    management_fee_bps  NUMBER(5,2),                -- Fee in basis points
    is_active           BOOLEAN         DEFAULT TRUE,
    load_ts             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    update_ts           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DWS_EDW.DWS_MASTER.DIM_ACCOUNT VALUES
    ('ACC001', 'CLI001', 'BVK Global Equity',             'SEGREGATED',    'EQUITY',       'EUR', '2018-04-01', 'BM001', 35.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC002', 'CLI001', 'BVK Euro Fixed Income',          'SEGREGATED',    'FIXED_INCOME', 'EUR', '2018-04-01', 'BM002', 20.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC003', 'CLI002', 'NRW Multi-Asset Balanced',       'SEGREGATED',    'MULTI_ASSET',  'EUR', '2017-07-01', 'BM003', 45.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC004', 'CLI003', 'Swiss Re Growth Fund',           'DISCRETIONARY', 'EQUITY',       'CHF', '2019-02-01', 'BM001', 50.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC005', 'CLI004', 'ADIA Global Opportunities',      'SEGREGATED',    'MULTI_ASSET',  'USD', '2016-10-01', 'BM003', 30.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC006', 'CLI004', 'ADIA Infrastructure Allocation', 'SEGREGATED',    'ALTERNATIVES', 'USD', '2017-01-15', 'BM004', 75.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC007', 'CLI005', 'CDPQ Canadian Bond Portfolio',   'SEGREGATED',    'FIXED_INCOME', 'CAD', '2018-12-01', 'BM002', 15.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC008', 'CLI006', 'USS UK Equity',                  'POOLED',        'EQUITY',       'GBP', '2017-03-15', 'BM005', 25.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC009', 'CLI007', 'Generali Euro Corp Bond',        'SEGREGATED',    'FIXED_INCOME', 'EUR', '2019-05-01', 'BM002', 18.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC010', 'CLI008', 'JP Insurance Stable Value',      'DISCRETIONARY', 'FIXED_INCOME', 'JPY', '2020-02-15', 'BM002', 12.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC011', 'CLI009', 'CalPERS US Large Cap',           'SEGREGATED',    'EQUITY',       'USD', '2015-08-01', 'BM006', 20.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC012', 'CLI009', 'CalPERS Global Fixed Income',    'SEGREGATED',    'FIXED_INCOME', 'USD', '2016-01-01', 'BM002', 15.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC013', 'CLI010', 'Temasek Asia Ex-Japan Equity',   'DISCRETIONARY', 'EQUITY',       'SGD', '2018-06-01', 'BM007', 40.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC014', 'CLI011', 'Harenberg Family Portfolio',     'ADVISORY',      'MULTI_ASSET',  'EUR', '2020-04-01', 'BM003', 60.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC015', 'CLI012', 'Fernandez Conservative Growth',  'DISCRETIONARY', 'MULTI_ASSET',  'EUR', '2021-07-01', 'BM003', 55.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC016', 'CLI013', 'Chen Aggressive Alpha',          'ADVISORY',      'EQUITY',       'USD', '2019-09-01', 'BM001', 70.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC017', 'CLI014', 'W&E Foundation Income',          'SEGREGATED',    'FIXED_INCOME', 'GBP', '2022-02-01', 'BM002', 15.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC018', 'CLI015', 'Allianz Treasury Pool',          'POOLED',        'FIXED_INCOME', 'EUR', '2016-12-15', 'BM002', 10.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC019', 'CLI016', 'OTPP Global Equity Core',        'SEGREGATED',    'EQUITY',       'CAD', '2017-05-01', 'BM001', 25.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC020', 'CLI017', 'NBIM Global Multi-Asset',        'SEGREGATED',    'MULTI_ASSET',  'NOK', '2015-02-01', 'BM003', 20.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC021', 'CLI018', 'GIC Asia Growth',                'DISCRETIONARY', 'EQUITY',       'SGD', '2016-09-01', 'BM007', 35.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC022', 'CLI019', 'Braun Savings Plan',             'POOLED',        'MULTI_ASSET',  'EUR', '2023-02-01', 'BM003', 80.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC023', 'CLI020', 'Laurent Balanced Fund',          'POOLED',        'MULTI_ASSET',  'EUR', '2023-04-01', 'BM003', 80.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC024', 'CLI021', 'KIA Strategic Allocation',       'SEGREGATED',    'MULTI_ASSET',  'USD', '2017-10-01', 'BM003', 30.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC025', 'CLI022', 'AP Pension Nordic Bond',         'SEGREGATED',    'FIXED_INCOME', 'DKK', '2020-01-01', 'BM002', 12.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC026', 'CLI023', 'Samsung Life Stable Return',     'DISCRETIONARY', 'FIXED_INCOME', 'KRW', '2020-06-15', 'BM002', 10.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC027', 'CLI024', 'Smith Growth ISA',               'POOLED',        'EQUITY',       'GBP', '2023-07-01', 'BM005', 75.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC028', 'CLI025', 'Mubadala Global Macro',          'SEGREGATED',    'MULTI_ASSET',  'USD', '2018-03-01', 'BM003', 40.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC029', 'CLI028', 'BPI France Innovation Fund',     'DISCRETIONARY', 'EQUITY',       'EUR', '2020-11-01', 'BM001', 45.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC030', 'CLI029', 'NPS Korea Diversified',          'SEGREGATED',    'MULTI_ASSET',  'KRW', '2017-04-01', 'BM003', 22.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC031', 'CLI030', 'Future Fund Pacific Growth',     'SEGREGATED',    'EQUITY',       'AUD', '2019-08-01', 'BM007', 30.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC032', 'CLI001', 'BVK Real Estate Allocation',     'SEGREGATED',    'ALTERNATIVES', 'EUR', '2020-01-01', 'BM004', 65.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC033', 'CLI002', 'NRW Green Bond Fund',            'SEGREGATED',    'FIXED_INCOME', 'EUR', '2021-03-01', 'BM002', 18.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC034', 'CLI005', 'CDPQ Infrastructure Income',     'SEGREGATED',    'ALTERNATIVES', 'CAD', '2019-06-01', 'BM004', 70.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC035', 'CLI006', 'USS Global Balanced',            'POOLED',        'MULTI_ASSET',  'GBP', '2018-09-01', 'BM003', 30.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC036', 'CLI010', 'Temasek Technology Growth',      'DISCRETIONARY', 'EQUITY',       'USD', '2021-01-15', 'BM006', 50.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC037', 'CLI017', 'NBIM Sovereign Bond',            'SEGREGATED',    'FIXED_INCOME', 'NOK', '2016-04-01', 'BM002', 8.00,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC038', 'CLI026', 'Defunct Holdings Portfolio',      'SEGREGATED',    'MULTI_ASSET',  'EUR', '2019-02-01', 'BM003', 40.00, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC039', 'CLI027', 'Closed Pension Bond Fund',       'SEGREGATED',    'FIXED_INCOME', 'GBP', '2018-07-01', 'BM002', 15.00, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('ACC040', 'CLI030', 'Future Fund Fixed Income',       'SEGREGATED',    'FIXED_INCOME', 'AUD', '2020-06-01', 'BM002', 12.00, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 3: DIM_FUND (Master Data - 20 funds/products)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_MASTER.DIM_FUND (
    fund_id             VARCHAR(20)     NOT NULL,
    isin                VARCHAR(12),
    fund_name           VARCHAR(200)    NOT NULL,
    fund_type           VARCHAR(30),                -- EQUITY, FIXED_INCOME, MULTI_ASSET, ETF, ALTERNATIVES
    asset_class         VARCHAR(50),
    fund_currency       VARCHAR(3),
    domicile            VARCHAR(5),
    inception_date      DATE,
    benchmark_id        VARCHAR(20),
    ter_bps             NUMBER(5,2),                -- Total Expense Ratio in bps
    is_active           BOOLEAN         DEFAULT TRUE,
    load_ts             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    update_ts           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DWS_EDW.DWS_MASTER.DIM_FUND VALUES
    ('FND001', 'DE0008490962', 'DWS Top Dividende',            'EQUITY',       'Global Equity',         'EUR', 'DE', '2003-04-28', 'BM001', 145, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND002', 'LU0599946893', 'DWS Invest Euro Corporate Bd', 'FIXED_INCOME', 'Euro Corp Bond',        'EUR', 'LU', '2011-07-18', 'BM002', 80,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND003', 'DE0008474024', 'DWS Deutschland',              'EQUITY',       'German Equity',         'EUR', 'DE', '1993-10-25', 'BM005', 140, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND004', 'LU1663929570', 'DWS ESG Euro Money Market',    'FIXED_INCOME', 'Money Market',          'EUR', 'LU', '2018-01-15', 'BM002', 15,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND005', 'DE000ETFL011', 'Xtrackers DAX ETF',            'ETF',          'German Equity',         'EUR', 'DE', '2007-01-11', 'BM005', 9,   TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND006', 'IE00BZ0PKT83', 'Xtrackers MSCI World ETF',     'ETF',          'Global Equity',         'USD', 'IE', '2014-07-22', 'BM001', 19,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND007', 'LU0274208692', 'Xtrackers S&P 500 ETF',        'ETF',          'US Equity',             'USD', 'LU', '2007-01-09', 'BM006', 7,   TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND008', 'LU1681043599', 'DWS Invest Asian Equities',    'EQUITY',       'Asia Ex-Japan Equity',  'USD', 'LU', '2006-06-19', 'BM007', 165, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND009', 'DE0008476250', 'DWS Akkumula',                 'EQUITY',       'Global Equity',         'EUR', 'DE', '1961-07-03', 'BM001', 135, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND010', 'LU0616839501', 'DWS Concept Kaldemorgen',      'MULTI_ASSET',  'Multi-Asset Balanced',  'EUR', 'LU', '2011-05-02', 'BM003', 155, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND011', 'LU1054320262', 'DWS Invest Green Bonds',       'FIXED_INCOME', 'Green Bond',            'EUR', 'LU', '2019-10-01', 'BM002', 65,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND012', 'DE0009769760', 'DWS Stiftungsfonds',           'MULTI_ASSET',  'Conservative Mix',      'EUR', 'DE', '2001-03-19', 'BM003', 95,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND013', 'LU2089238625', 'DWS Invest Infrastructure',    'ALTERNATIVES', 'Infrastructure',        'EUR', 'LU', '2020-06-15', 'BM004', 180, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND014', 'IE00BM67HT60', 'Xtrackers Japan ETF',          'ETF',          'Japan Equity',          'JPY', 'IE', '2010-01-20', 'BM007', 12,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND015', 'LU1399300455', 'DWS Floating Rate Notes',      'FIXED_INCOME', 'Floating Rate',         'EUR', 'LU', '2016-03-14', 'BM002', 30,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND016', 'DE000DWS2US9', 'DWS US Growth',                'EQUITY',       'US Equity Growth',      'USD', 'DE', '2015-09-01', 'BM006', 150, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND017', 'LU2363800844', 'DWS Invest Real Estate Global','ALTERNATIVES', 'Real Estate',           'EUR', 'LU', '2021-04-01', 'BM004', 170, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND018', 'IE00BK5BCD43', 'Xtrackers EM Bond ETF',        'ETF',          'EM Fixed Income',       'USD', 'IE', '2013-11-01', 'BM002', 25,  TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND019', 'LU0511516738', 'DWS Invest Global Agribiz',    'EQUITY',       'Thematic Equity',       'EUR', 'LU', '2010-08-02', 'BM001', 160, TRUE,  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('FND020', 'DE000DK2CDS0', 'DWS Retired Fund XYZ',         'EQUITY',       'Global Equity',         'EUR', 'DE', '2008-01-01', 'BM001', 140, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 4: DIM_BENCHMARK (Reference Data - 10 benchmarks)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_REF.DIM_BENCHMARK (
    benchmark_id        VARCHAR(20)     NOT NULL,
    benchmark_name      VARCHAR(200)    NOT NULL,
    benchmark_ticker    VARCHAR(30),
    asset_class         VARCHAR(50),
    currency            VARCHAR(3),
    provider            VARCHAR(50),
    load_ts             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DWS_EDW.DWS_REF.DIM_BENCHMARK VALUES
    ('BM001', 'MSCI World Net Total Return',      'NDDUWI',   'Global Equity',        'USD', 'MSCI',       CURRENT_TIMESTAMP()),
    ('BM002', 'Bloomberg Euro Aggregate Bond',     'LBEATREU', 'Euro Fixed Income',    'EUR', 'Bloomberg',  CURRENT_TIMESTAMP()),
    ('BM003', '50% MSCI World / 50% Euro Agg',    'CUSTOM01', 'Multi-Asset Balanced',  'EUR', 'Custom',     CURRENT_TIMESTAMP()),
    ('BM004', 'FTSE EPRA/NAREIT Developed',        'RUGL',     'Real Assets',          'USD', 'FTSE',       CURRENT_TIMESTAMP()),
    ('BM005', 'DAX Total Return',                  'GDAXI',    'German Equity',        'EUR', 'Deutsche Boerse', CURRENT_TIMESTAMP()),
    ('BM006', 'S&P 500 Total Return',              'SPXT',     'US Equity',            'USD', 'S&P',        CURRENT_TIMESTAMP()),
    ('BM007', 'MSCI AC Asia Ex-Japan',             'MXASJ',    'Asia Ex-Japan Equity', 'USD', 'MSCI',       CURRENT_TIMESTAMP()),
    ('BM008', 'EURIBOR 3M',                        'EUR003M',  'Cash/Money Market',    'EUR', 'EMMI',       CURRENT_TIMESTAMP()),
    ('BM009', 'ICE BofA Global HY',                'HW00',     'High Yield Bond',      'USD', 'ICE',        CURRENT_TIMESTAMP()),
    ('BM010', 'MSCI Emerging Markets',             'MXEF',     'EM Equity',            'USD', 'MSCI',       CURRENT_TIMESTAMP());


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 5: FACT_PORTFOLIO_HOLDINGS (~500 records)
-- Daily positions: account × fund × date
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS (
    holding_date        DATE            NOT NULL,
    account_id          VARCHAR(20)     NOT NULL,
    fund_id             VARCHAR(20)     NOT NULL,
    quantity            NUMBER(18,6)    NOT NULL,   -- Units/shares held
    cost_price          NUMBER(18,6),               -- Average cost per unit
    cost_value_local    NUMBER(18,2),               -- Total cost in fund currency
    market_value_local  NUMBER(18,2),               -- Total market value in fund currency
    currency            VARCHAR(3),
    source_system       VARCHAR(20)     DEFAULT 'SIMON',  -- DWS portfolio system
    created_ts          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_ts          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Generate holdings for multiple dates (Jan 2024 - Feb 2024)
-- Using representative positions across accounts and funds
INSERT INTO DWS_EDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS
WITH date_series AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-02')::DATE AS holding_date
    FROM TABLE(GENERATOR(ROWCOUNT => 40))
    WHERE DATEADD('day', SEQ4(), '2024-01-02')::DATE <= '2024-02-28'
      AND DAYOFWEEK(DATEADD('day', SEQ4(), '2024-01-02')::DATE) NOT IN (0, 6)
),
account_fund_pairs AS (
    SELECT * FROM (VALUES
        ('ACC001', 'FND001', 150000.000000, 95.50,  14325000.00, 'EUR'),
        ('ACC001', 'FND006', 80000.000000,  42.30,  3384000.00,  'USD'),
        ('ACC002', 'FND002', 200000.000000, 101.20, 20240000.00, 'EUR'),
        ('ACC002', 'FND011', 100000.000000, 98.50,  9850000.00,  'EUR'),
        ('ACC003', 'FND010', 120000.000000, 155.80, 18696000.00, 'EUR'),
        ('ACC003', 'FND001', 50000.000000,  95.50,  4775000.00,  'EUR'),
        ('ACC004', 'FND001', 75000.000000,  95.50,  7162500.00,  'CHF'),
        ('ACC004', 'FND008', 60000.000000,  28.40,  1704000.00,  'USD'),
        ('ACC005', 'FND006', 200000.000000, 42.30,  8460000.00,  'USD'),
        ('ACC005', 'FND010', 90000.000000,  155.80, 14022000.00, 'EUR'),
        ('ACC007', 'FND002', 180000.000000, 101.20, 18216000.00, 'CAD'),
        ('ACC008', 'FND003', 100000.000000, 280.60, 28060000.00, 'GBP'),
        ('ACC009', 'FND002', 250000.000000, 101.20, 25300000.00, 'EUR'),
        ('ACC011', 'FND007', 300000.000000, 510.20, 153060000.00,'USD'),
        ('ACC011', 'FND016', 120000.000000, 78.90,  9468000.00,  'USD'),
        ('ACC013', 'FND008', 180000.000000, 28.40,  5112000.00,  'SGD'),
        ('ACC014', 'FND010', 25000.000000,  155.80, 3895000.00,  'EUR'),
        ('ACC014', 'FND001', 15000.000000,  95.50,  1432500.00,  'EUR'),
        ('ACC018', 'FND004', 500000.000000, 100.05, 50025000.00, 'EUR'),
        ('ACC020', 'FND010', 160000.000000, 155.80, 24928000.00, 'NOK'),
        ('ACC020', 'FND006', 110000.000000, 42.30,  4653000.00,  'USD'),
        ('ACC024', 'FND010', 100000.000000, 155.80, 15580000.00, 'USD'),
        ('ACC028', 'FND006', 250000.000000, 42.30,  10575000.00, 'USD')
    ) AS t(account_id, fund_id, quantity, cost_price, cost_value_local, currency)
)
SELECT
    d.holding_date,
    p.account_id,
    p.fund_id,
    p.quantity + (UNIFORM(-500, 500, RANDOM()) * 0.1) AS quantity,
    p.cost_price,
    p.cost_value_local,
    p.cost_value_local * (1 + (UNIFORM(-300, 500, RANDOM()) * 0.0001)) AS market_value_local,
    p.currency,
    'SIMON',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM date_series d
CROSS JOIN account_fund_pairs p;


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 6: FACT_TRANSACTIONS (~300 records)
-- Buy/Sell/Dividend/Fee transactions
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_TRAN.FACT_TRANSACTIONS (
    transaction_id      VARCHAR(30)     NOT NULL,
    transaction_date    DATE            NOT NULL,
    settlement_date     DATE,
    account_id          VARCHAR(20)     NOT NULL,
    fund_id             VARCHAR(20)     NOT NULL,
    transaction_type    VARCHAR(20)     NOT NULL,   -- BUY, SELL, DIVIDEND, FEE, TRANSFER_IN, TRANSFER_OUT
    quantity            NUMBER(18,6),
    price_per_unit      NUMBER(18,6),
    gross_amount        NUMBER(18,2),
    fees                NUMBER(18,2)    DEFAULT 0,
    tax_amount          NUMBER(18,2)    DEFAULT 0,
    net_amount          NUMBER(18,2),
    currency            VARCHAR(3),
    source_system       VARCHAR(20)     DEFAULT 'SIMON',
    created_ts          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_ts          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DWS_EDW.DWS_TRAN.FACT_TRANSACTIONS VALUES
    -- January 2024 transactions
    ('TXN00001', '2024-01-03', '2024-01-05', 'ACC001', 'FND001', 'BUY',          5000.000000,  96.20,  481000.00,   240.50,  0.00,     480759.50,  'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00002', '2024-01-03', '2024-01-05', 'ACC001', 'FND006', 'BUY',          3000.000000,  43.10,  129300.00,   64.65,   0.00,     129235.35,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00003', '2024-01-04', '2024-01-08', 'ACC002', 'FND002', 'BUY',          10000.000000, 101.50, 1015000.00,  507.50,  0.00,     1014492.50, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00004', '2024-01-05', '2024-01-09', 'ACC003', 'FND010', 'BUY',          2000.000000,  156.20, 312400.00,   156.20,  0.00,     312243.80,  'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00005', '2024-01-08', '2024-01-10', 'ACC005', 'FND006', 'BUY',          8000.000000,  43.50,  348000.00,   174.00,  0.00,     347826.00,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00006', '2024-01-09', '2024-01-11', 'ACC008', 'FND003', 'BUY',          1500.000000,  282.40, 423600.00,   211.80,  0.00,     423388.20,  'GBP', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00007', '2024-01-10', '2024-01-12', 'ACC011', 'FND007', 'BUY',          5000.000000,  512.30, 2561500.00,  1280.75, 0.00,     2560219.25, 'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00008', '2024-01-10', '2024-01-12', 'ACC014', 'FND010', 'BUY',          500.000000,   156.50, 78250.00,    39.13,   0.00,     78210.88,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00009', '2024-01-12', '2024-01-16', 'ACC013', 'FND008', 'BUY',          4000.000000,  29.10,  116400.00,   58.20,   0.00,     116341.80,  'SGD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00010', '2024-01-15', '2024-01-17', 'ACC001', 'FND001', 'DIVIDEND',     0.000000,     0.00,   22500.00,    0.00,    3375.00,  19125.00,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00011', '2024-01-15', '2024-01-17', 'ACC011', 'FND007', 'DIVIDEND',     0.000000,     0.00,   45000.00,    0.00,    6750.00,  38250.00,   'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00012', '2024-01-16', '2024-01-18', 'ACC004', 'FND001', 'BUY',          2000.000000,  97.10,  194200.00,   97.10,   0.00,     194102.90,  'CHF', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00013', '2024-01-17', '2024-01-19', 'ACC020', 'FND010', 'BUY',          3000.000000,  157.30, 471900.00,   235.95,  0.00,     471664.05,  'NOK', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00014', '2024-01-18', '2024-01-22', 'ACC007', 'FND002', 'BUY',          8000.000000,  101.80, 814400.00,   407.20,  0.00,     813992.80,  'CAD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00015', '2024-01-19', '2024-01-23', 'ACC009', 'FND002', 'BUY',          12000.000000, 101.60, 1219200.00,  609.60,  0.00,     1218590.40, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00016', '2024-01-22', '2024-01-24', 'ACC001', 'FND001', 'SELL',         -2000.000000, 97.80,  -195600.00,  97.80,   0.00,     -195697.80, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00017', '2024-01-22', '2024-01-24', 'ACC005', 'FND010', 'BUY',          1500.000000,  157.00, 235500.00,   117.75,  0.00,     235382.25,  'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00018', '2024-01-23', '2024-01-25', 'ACC018', 'FND004', 'BUY',          50000.000000, 100.08, 5004000.00,  2502.00, 0.00,     5001498.00, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00019', '2024-01-24', '2024-01-26', 'ACC024', 'FND010', 'BUY',          2500.000000,  157.50, 393750.00,   196.88,  0.00,     393553.13,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00020', '2024-01-25', '2024-01-29', 'ACC028', 'FND006', 'BUY',          6000.000000,  44.20,  265200.00,   132.60,  0.00,     265067.40,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00021', '2024-01-26', '2024-01-30', 'ACC011', 'FND016', 'BUY',          3000.000000,  79.50,  238500.00,   119.25,  0.00,     238380.75,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00022', '2024-01-29', '2024-01-31', 'ACC003', 'FND001', 'BUY',          1000.000000,  98.20,  98200.00,    49.10,   0.00,     98150.90,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00023', '2024-01-30', '2024-02-01', 'ACC004', 'FND008', 'BUY',          5000.000000,  29.50,  147500.00,   73.75,   0.00,     147426.25,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00024', '2024-01-31', NULL,         'ACC001', 'FND001', 'FEE',          0.000000,     0.00,   -4178.13,    0.00,    0.00,     -4178.13,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00025', '2024-01-31', NULL,         'ACC002', 'FND002', 'FEE',          0.000000,     0.00,   -3373.33,    0.00,    0.00,     -3373.33,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00026', '2024-01-31', NULL,         'ACC011', 'FND007', 'FEE',          0.000000,     0.00,   -25510.00,   0.00,    0.00,     -25510.00,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    -- February 2024 transactions
    ('TXN00027', '2024-02-01', '2024-02-05', 'ACC001', 'FND006', 'SELL',         -1500.000000, 44.80,  -67200.00,   33.60,   0.00,     -67233.60,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00028', '2024-02-02', '2024-02-06', 'ACC008', 'FND003', 'SELL',         -500.000000,  285.20, -142600.00,  71.30,   0.00,     -142671.30, 'GBP', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00029', '2024-02-05', '2024-02-07', 'ACC005', 'FND006', 'BUY',          4000.000000,  44.60,  178400.00,   89.20,   0.00,     178310.80,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00030', '2024-02-06', '2024-02-08', 'ACC020', 'FND006', 'BUY',          5000.000000,  44.90,  224500.00,   112.25,  0.00,     224387.75,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00031', '2024-02-07', '2024-02-09', 'ACC003', 'FND010', 'SELL',         -1000.000000, 158.40, -158400.00,  79.20,   0.00,     -158479.20, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00032', '2024-02-08', '2024-02-12', 'ACC013', 'FND008', 'BUY',          3000.000000,  29.80,  89400.00,    44.70,   0.00,     89355.30,   'SGD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00033', '2024-02-09', '2024-02-13', 'ACC011', 'FND007', 'BUY',          2000.000000,  515.60, 1031200.00,  515.60,  0.00,     1030684.40, 'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00034', '2024-02-12', '2024-02-14', 'ACC014', 'FND001', 'BUY',          300.000000,   98.80,  29640.00,    14.82,   0.00,     29625.18,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00035', '2024-02-13', '2024-02-15', 'ACC009', 'FND002', 'SELL',         -5000.000000, 102.10, -510500.00,  255.25,  0.00,     -510755.25, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00036', '2024-02-14', '2024-02-16', 'ACC024', 'FND010', 'BUY',          1500.000000,  158.80, 238200.00,   119.10,  0.00,     238080.90,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00037', '2024-02-15', '2024-02-19', 'ACC001', 'FND001', 'DIVIDEND',     0.000000,     0.00,   23400.00,    0.00,    3510.00,  19890.00,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00038', '2024-02-15', '2024-02-19', 'ACC008', 'FND003', 'DIVIDEND',     0.000000,     0.00,   14200.00,    0.00,    2130.00,  12070.00,   'GBP', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00039', '2024-02-16', '2024-02-20', 'ACC028', 'FND006', 'SELL',         -3000.000000, 45.30,  -135900.00,  67.95,   0.00,     -135967.95, 'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00040', '2024-02-19', '2024-02-21', 'ACC007', 'FND002', 'SELL',         -4000.000000, 102.30, -409200.00,  204.60,  0.00,     -409404.60, 'CAD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00041', '2024-02-20', '2024-02-22', 'ACC004', 'FND001', 'SELL',         -500.000000,  99.20,  -49600.00,   24.80,   0.00,     -49624.80,  'CHF', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00042', '2024-02-21', '2024-02-23', 'ACC005', 'FND010', 'SELL',         -800.000000,  159.10, -127280.00,  63.64,   0.00,     -127343.64, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00043', '2024-02-22', '2024-02-26', 'ACC018', 'FND004', 'BUY',          30000.000000, 100.10, 3003000.00,  1501.50, 0.00,     3001498.50, 'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00044', '2024-02-23', '2024-02-27', 'ACC020', 'FND010', 'BUY',          2000.000000,  159.50, 319000.00,   159.50,  0.00,     318840.50,  'NOK', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00045', '2024-02-26', '2024-02-28', 'ACC011', 'FND016', 'SELL',         -1000.000000, 81.20,  -81200.00,   40.60,   0.00,     -81240.60,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00046', '2024-02-27', '2024-02-29', 'ACC003', 'FND001', 'DIVIDEND',     0.000000,     0.00,   7500.00,     0.00,    1125.00,  6375.00,    'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00047', '2024-02-28', NULL,         'ACC001', 'FND001', 'FEE',          0.000000,     0.00,   -4320.00,    0.00,    0.00,     -4320.00,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00048', '2024-02-28', NULL,         'ACC005', 'FND006', 'FEE',          0.000000,     0.00,   -2115.00,    0.00,    0.00,     -2115.00,   'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00049', '2024-02-28', NULL,         'ACC011', 'FND007', 'FEE',          0.000000,     0.00,   -26200.00,   0.00,    0.00,     -26200.00,  'USD', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('TXN00050', '2024-02-28', '2024-03-01', 'ACC001', 'FND001', 'TRANSFER_IN',  1000.000000,  99.50,  99500.00,    0.00,    0.00,     99500.00,   'EUR', 'SIMON', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 7: FACT_NAV_PRICES (~400 records)
-- Daily NAV per fund
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_TRAN.FACT_NAV_PRICES (
    price_date          DATE            NOT NULL,
    fund_id             VARCHAR(20)     NOT NULL,
    nav_per_unit        NUMBER(18,6)    NOT NULL,
    daily_return_pct    NUMBER(10,6),
    ytd_return_pct      NUMBER(10,6),
    currency            VARCHAR(3),
    source_system       VARCHAR(20)     DEFAULT 'BLOOMBERG',
    created_ts          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Generate NAV prices for 20 funds across ~40 business days
INSERT INTO DWS_EDW.DWS_TRAN.FACT_NAV_PRICES
WITH date_series AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-02')::DATE AS price_date
    FROM TABLE(GENERATOR(ROWCOUNT => 60))
    WHERE DATEADD('day', SEQ4(), '2024-01-02')::DATE <= '2024-02-29'
      AND DAYOFWEEK(DATEADD('day', SEQ4(), '2024-01-02')::DATE) NOT IN (0, 6)
),
fund_base AS (
    SELECT * FROM (VALUES
        ('FND001', 96.00,  'EUR'), ('FND002', 101.50, 'EUR'), ('FND003', 281.00, 'EUR'),
        ('FND004', 100.05, 'EUR'), ('FND005', 16800.00,'EUR'),('FND006', 43.20,  'USD'),
        ('FND007', 511.00, 'USD'), ('FND008', 28.80,  'USD'), ('FND009', 1420.00,'EUR'),
        ('FND010', 156.00, 'EUR'), ('FND011', 98.70,  'EUR'), ('FND012', 48.30,  'EUR'),
        ('FND013', 102.50, 'EUR'), ('FND014', 18500.00,'JPY'),('FND015', 100.20, 'EUR'),
        ('FND016', 79.00,  'USD'), ('FND017', 51.80,  'EUR'), ('FND018', 8.50,   'USD'),
        ('FND019', 135.60, 'EUR'), ('FND020', 0.00,   'EUR')
    ) AS t(fund_id, base_nav, currency)
    WHERE base_nav > 0
)
SELECT
    d.price_date,
    f.fund_id,
    ROUND(f.base_nav * (1 + (UNIFORM(-200, 300, RANDOM()) * 0.0001) * 
        (DATEDIFF('day', '2024-01-02', d.price_date) + 1)), 6) AS nav_per_unit,
    ROUND(UNIFORM(-200, 300, RANDOM()) * 0.0001, 6) AS daily_return_pct,
    ROUND(UNIFORM(-500, 800, RANDOM()) * 0.0001, 6) AS ytd_return_pct,
    f.currency,
    'BLOOMBERG',
    CURRENT_TIMESTAMP()
FROM date_series d
CROSS JOIN fund_base f;


-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE 8: FACT_FX_RATES (~200 records)
-- Daily FX rates to EUR (base currency)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE DWS_EDW.DWS_REF.FACT_FX_RATES (
    rate_date           DATE            NOT NULL,
    from_currency       VARCHAR(3)      NOT NULL,
    to_currency         VARCHAR(3)      NOT NULL DEFAULT 'EUR',
    exchange_rate       NUMBER(18,8)    NOT NULL,
    source_system       VARCHAR(20)     DEFAULT 'ECB',
    created_ts          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DWS_EDW.DWS_REF.FACT_FX_RATES
WITH date_series AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-02')::DATE AS rate_date
    FROM TABLE(GENERATOR(ROWCOUNT => 60))
    WHERE DATEADD('day', SEQ4(), '2024-01-02')::DATE <= '2024-02-29'
      AND DAYOFWEEK(DATEADD('day', SEQ4(), '2024-01-02')::DATE) NOT IN (0, 6)
),
currency_base AS (
    SELECT * FROM (VALUES
        ('USD', 0.92000000), ('GBP', 1.15500000), ('CHF', 1.06000000),
        ('JPY', 0.00620000), ('CAD', 0.68500000), ('SGD', 0.68000000),
        ('AUD', 0.60500000), ('NOK', 0.08800000), ('DKK', 0.13400000),
        ('KRW', 0.00071000)
    ) AS t(from_currency, base_rate)
)
SELECT
    d.rate_date,
    c.from_currency,
    'EUR',
    ROUND(c.base_rate * (1 + UNIFORM(-100, 100, RANDOM()) * 0.00001), 8) AS exchange_rate,
    'ECB',
    CURRENT_TIMESTAMP()
FROM date_series d
CROSS JOIN currency_base c;

-- Add EUR to EUR (1:1) for completeness
INSERT INTO DWS_EDW.DWS_REF.FACT_FX_RATES
SELECT
    DATEADD('day', SEQ4(), '2024-01-02')::DATE AS rate_date,
    'EUR', 'EUR', 1.00000000, 'ECB', CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 60))
WHERE DATEADD('day', SEQ4(), '2024-01-02')::DATE <= '2024-02-29'
  AND DAYOFWEEK(DATEADD('day', SEQ4(), '2024-01-02')::DATE) NOT IN (0, 6);


-- ═══════════════════════════════════════════════════════════════════════════════
-- DEV & TEST ENVIRONMENT SETUP (clone from prod)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS DWS_EDWDEV;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_TRAN   CLONE DWS_EDW.DWS_TRAN;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_MASTER CLONE DWS_EDW.DWS_MASTER;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_REF    CLONE DWS_EDW.DWS_REF;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_CLIENT_REPORTING;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_SNAPSHOTS;

CREATE DATABASE IF NOT EXISTS DWS_EDWTEST;
CREATE SCHEMA IF NOT EXISTS DWS_EDWTEST.DWS_TRAN   CLONE DWS_EDW.DWS_TRAN;
CREATE SCHEMA IF NOT EXISTS DWS_EDWTEST.DWS_MASTER CLONE DWS_EDW.DWS_MASTER;
CREATE SCHEMA IF NOT EXISTS DWS_EDWTEST.DWS_REF    CLONE DWS_EDW.DWS_REF;
CREATE SCHEMA IF NOT EXISTS DWS_EDWTEST.DWS_CLIENT_REPORTING;
CREATE SCHEMA IF NOT EXISTS DWS_EDWTEST.DWS_SNAPSHOTS;


-- ═══════════════════════════════════════════════════════════════════════════════
-- ROLE & PERMISSION SETUP
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS DWS_DEVELOPER;
CREATE ROLE IF NOT EXISTS DWS_TESTER;
CREATE ROLE IF NOT EXISTS DWS_PROD;

GRANT ROLE DWS_DEVELOPER TO ROLE SYSADMIN;
GRANT ROLE DWS_TESTER    TO ROLE SYSADMIN;
GRANT ROLE DWS_PROD      TO ROLE SYSADMIN;

-- Dev permissions
GRANT USAGE ON DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT SELECT ON ALL TABLES IN DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT CREATE SCHEMA ON DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;
GRANT SELECT ON FUTURE TABLES IN DATABASE DWS_EDWDEV TO ROLE DWS_DEVELOPER;

-- Test permissions
GRANT USAGE ON DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT SELECT ON ALL TABLES IN DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT CREATE SCHEMA ON DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;
GRANT SELECT ON FUTURE TABLES IN DATABASE DWS_EDWTEST TO ROLE DWS_TESTER;

-- Prod permissions
GRANT USAGE ON DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT SELECT ON ALL TABLES IN DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT CREATE SCHEMA ON DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DWS_EDW TO ROLE DWS_PROD;
GRANT SELECT ON FUTURE TABLES IN DATABASE DWS_EDW TO ROLE DWS_PROD;

-- Prod read access for dev/test roles (to read sources)
GRANT USAGE ON DATABASE DWS_EDW TO ROLE DWS_DEVELOPER;
GRANT USAGE ON DATABASE DWS_EDW TO ROLE DWS_TESTER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DWS_EDW TO ROLE DWS_DEVELOPER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DWS_EDW TO ROLE DWS_TESTER;
GRANT SELECT ON ALL TABLES IN DATABASE DWS_EDW TO ROLE DWS_DEVELOPER;
GRANT SELECT ON ALL TABLES IN DATABASE DWS_EDW TO ROLE DWS_TESTER;

USE ROLE SYSADMIN;


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT 'DIM_CLIENT'             AS table_name, COUNT(*) AS row_count FROM DWS_EDW.DWS_MASTER.DIM_CLIENT
UNION ALL
SELECT 'DIM_ACCOUNT',           COUNT(*) FROM DWS_EDW.DWS_MASTER.DIM_ACCOUNT
UNION ALL
SELECT 'DIM_FUND',              COUNT(*) FROM DWS_EDW.DWS_MASTER.DIM_FUND
UNION ALL
SELECT 'DIM_BENCHMARK',         COUNT(*) FROM DWS_EDW.DWS_REF.DIM_BENCHMARK
UNION ALL
SELECT 'FACT_PORTFOLIO_HOLDINGS',COUNT(*) FROM DWS_EDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS
UNION ALL
SELECT 'FACT_TRANSACTIONS',     COUNT(*) FROM DWS_EDW.DWS_TRAN.FACT_TRANSACTIONS
UNION ALL
SELECT 'FACT_NAV_PRICES',       COUNT(*) FROM DWS_EDW.DWS_TRAN.FACT_NAV_PRICES
UNION ALL
SELECT 'FACT_FX_RATES',         COUNT(*) FROM DWS_EDW.DWS_REF.FACT_FX_RATES
ORDER BY table_name;

SELECT 'PROD: DWS_EDW'   AS env, COUNT(*) AS schemas FROM DWS_EDW.INFORMATION_SCHEMA.SCHEMATA
UNION ALL
SELECT 'DEV: DWS_EDWDEV',        COUNT(*) FROM DWS_EDWDEV.INFORMATION_SCHEMA.SCHEMATA
UNION ALL
SELECT 'TEST: DWS_EDWTEST',      COUNT(*) FROM DWS_EDWTEST.INFORMATION_SCHEMA.SCHEMATA;
