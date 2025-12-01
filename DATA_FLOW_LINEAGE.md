# Data Flow & Lineage Guide

**Project:** Honeywell PoC - DBT Projects  
**Date:** December 1, 2025  
**Status:** ✅ Production Ready

---

## 📊 Overview

This document provides a complete view of data lineage from **4 source tables** through **4 transformation layers** to produce **2 business-ready AR aging reports**.

### Quick Summary

| Aspect | Details |
|--------|---------|
| **Initial Source Tables** | 4 tables |
| **Transformation Layers** | 4 layers (Source → Staging → Dimension → Mart) |
| **Staging Models** | 1 model |
| **Dimension Models** | 2 models |
| **Final Marts** | 2 AR aging reports |
| **dbt Projects** | 2 projects (foundation + finance_core) |
| **Total Data Assets** | 9 data objects (4 sources + 5 dbt models) |

---

## 🗂️ Source Tables (Layer 1)

### **Total Source Tables: 4**

All source tables reside in the **EDW** database.

#### 1️⃣ Transaction Data

**Table:** `EDW.CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL`

```yaml
Purpose: Global accounts receivable fact table
Schema: CORP_TRAN
Row Count: Variable (production data)
Grain: One row per invoice line item
Key Fields:
  - source_system (BRP900, CIP900, CIP300, EEP300, P11, PRD010, C11111, ARP900)
  - company_code
  - accounting_doc (invoice number)
  - account_doc_line_item
  - fiscal_year
```

**Key Attributes:**
- **Amounts:** amt_usd, amt_usd_me, amt_doc, amt_lcl
- **Dates:** doc_date, posting_date, net_due_date, baseline_date, clearing_date
- **Customer:** sold_to, customer_num_sk
- **GL:** gl_account, sub_gl_account, profit_center, sales_org
- **Payment:** payment_terms, payment_terms_name
- **Status:** account_type (D/C), clearing_date (NULL = open)

**Sample Data:** 500 records via `LOAD_SAMPLE_SOURCE_DATA.sql` → `AR_INVOICE_OPEN`

---

#### 2️⃣ Master Data

**Table:** `EDW.CORP_MASTER.DIM_CUSTOMER`

```yaml
Purpose: Customer master data from all source systems
Schema: CORP_MASTER
Row Count: 100+ customers
Grain: One row per customer per source system
Key Fields:
  - customer_num_sk (unique per source system)
  - source_system
```

**Key Attributes:**
- **Identity:** customer_name, customer_type (E/I), customer_classification
- **Location:** customer_country, customer_country_name
- **Hierarchy:** customer_account_group
- **MDM:** duns_number, global_ultimate_duns, global_ultimate_name
- **Metadata:** load_ts, update_ts

**Sample Data:** 100 records via `LOAD_SAMPLE_SOURCE_DATA.sql`

---

#### 3️⃣ Master Data - Entities

**Table:** `EDW.CORP_MASTER.DIM_ENTITY`

```yaml
Purpose: Legal entity master data
Schema: CORP_MASTER
Row Count: 8 entities
Grain: One row per legal entity per source system
Key Fields:
  - source_entity_code_sk (company code)
  - source_system
```

**Key Attributes:**
- **Identity:** entity_name, entity_country_name
- **Geography:** entity_global_region, entity_global_sub_region, entity_global_sub_region_name
- **Categorization:** entity_region_category, entity_region_sub_category
- **Status:** entity_status (Active/Inactive)
- **Metadata:** load_ts, update_ts

**Sample Data:** 8 records via `LOAD_SAMPLE_SOURCE_DATA.sql`
- BRP900: 2 entities (US, Canada)
- CIP900: 3 entities (Germany, France, UK)
- CIP300: 3 entities (Singapore, China, Japan)

---

#### 4️⃣ Reference Data

**Table:** `EDW.CORP_REF.TIME_FISCAL_DAY`

```yaml
Purpose: Fiscal calendar with day-level granularity
Schema: CORP_REF
Row Count: 730 days (2024-2025)
Grain: One row per calendar day
Key Field: fiscal_day_key_str (YYYYMMDD)
```

**Key Attributes:**
- **Fiscal:** fiscal_year_int, fiscal_period_int, fiscal_year_period_str, fiscal_year_quarter_str
- **Calendar:** calendar_year, calendar_month, calendar_day, day_of_week
- **Date:** fiscal_date_key_date (DATE type)
- **Metadata:** load_ts

**Sample Data:** 730 records via `LOAD_SAMPLE_SOURCE_DATA.sql`

---

## 🔄 Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LAYER 1: SOURCE TABLES                              │
│                         Database: EDW                                       │
│                         Projects: None (source system)                      │
└─────────────────────────────────────────────────────────────────────────────┘

   ┌─────────────────────────────┐
   │  EDW.CORP_TRAN              │
   │  ─────────────────────────  │
   │  FACT_ACCOUNT_RECEIVABLE_   │
   │  GBL                        │
   │  • All AR transactions      │
   │  • 3 source systems         │
   │  • Open & cleared items     │
   └──────────┬──────────────────┘
              │
              │         ┌─────────────────────────┐
              │         │  EDW.CORP_MASTER        │
              │         │  ─────────────────────  │
              │         │  DIM_CUSTOMER           │
              │         │  • Customer master      │
              │         │  • 100 customers        │
              │         │  • Multi-source         │
              │         └──────────┬──────────────┘
              │                    │
              │         ┌──────────┴──────────────┐
              │         │  EDW.CORP_MASTER        │
              │         │  ─────────────────────  │
              │         │  DIM_ENTITY             │
              │         │  • Legal entities       │
              │         │  • 8 entities           │
              │         │  • BRP/CIP/CIP300       │
              │         └──────────┬──────────────┘
              │                    │
              │                    │        ┌──────────────────────┐
              │                    │        │  EDW.CORP_REF        │
              │                    │        │  ──────────────────  │
              │                    │        │  TIME_FISCAL_DAY     │
              │                    │        │  • Fiscal calendar   │
              │                    │        │  • 730 days          │
              │                    │        │  • 2024-2025         │
              │                    │        └───────┬──────────────┘
              │                    │                │
              ▼                    ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LAYER 2: STAGING LAYER (dbt_foundation)                  │
│                    Schema: EDW.DEV_DBT_DBT_STAGING                          │
│                    Project: dbt_foundation                                  │
└─────────────────────────────────────────────────────────────────────────────┘

              ┌──────────────────────────────────────┐
              │  STG_AR_INVOICE                      │
              │  ──────────────────────────────────  │
              │  Source: FACT_ACCOUNT_RECEIVABLE_GBL │
              │  Type: VIEW                          │
              │  ────────────────────────────────    │
              │  Filters:                            │
              │  ✓ clearing_date IS NULL (open)      │
              │  ✓ account_type = 'D' (debits)       │
              │  ✓ No special GL indicators          │
              │  ────────────────────────────────    │
              │  Transformations:                    │
              │  • Rename columns                    │
              │  • Add _stg_loaded_at                │
              │  • Standard naming conventions       │
              └──────────────┬───────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  LAYER 3: DIMENSION LAYER (dbt_foundation)                  │
│                  Schema: EDW.DEV_DBT_DBT_SHARED                             │
│                  Project: dbt_foundation                                    │
└─────────────────────────────────────────────────────────────────────────────┘

              ┌──────────────────────────────────────┐
              │  DIM_CUSTOMER                        │
              │  ──────────────────────────────────  │
              │  Source: CORP_MASTER.DIM_CUSTOMER    │
              │  Type: TABLE (SCD Type 1)            │
              │  ────────────────────────────────    │
              │  Transformations:                    │
              │  • Composite key: customer_id        │
              │  • Add is_internal flag              │
              │  • Rename MDM fields                 │
              │  • Add _loaded_at                    │
              └──────────────┬───────────────────────┘
                             │
              ┌──────────────┴───────────────────────┐
              │  DIM_FISCAL_CALENDAR                 │
              │  ──────────────────────────────────  │
              │  Source: CORP_REF.TIME_FISCAL_DAY    │
              │  Type: TABLE                         │
              │  ────────────────────────────────    │
              │  Transformations:                    │
              │  • Rename fiscal_date field          │
              │  • Add _loaded_at                    │
              │  • Standardize naming                │
              └──────────────┬───────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LAYER 4: MART LAYER (dbt_finance_core)                   │
│                    Schema: EDW.DEV_DBT_DBT_FINANCE                          │
│                    Project: dbt_finance_core                                │
└─────────────────────────────────────────────────────────────────────────────┘

              ┌──────────────────────────────────────┐
              │  DM_FIN_AR_AGING_SIMPLE              │
              │  ──────────────────────────────────  │
              │  Sources:                            │
              │  • STG_AR_INVOICE                    │
              │  • DIM_CUSTOMER (LEFT JOIN)          │
              │  • DIM_FISCAL_CALENDAR (LEFT JOIN)   │
              │  Type: TABLE                         │
              │  ────────────────────────────────    │
              │  Business Logic:                     │
              │  • Days late calculation             │
              │  • Aging buckets (9 buckets)         │
              │  • Current vs past due amounts       │
              │  • Customer type flags               │
              │  • Fiscal period attribution         │
              └──────────────────────────────────────┘

              ┌──────────────────────────────────────┐
              │  DM_FIN_AR_AGING_SIMPLE_V2           │
              │  ──────────────────────────────────  │
              │  Sources: Same as above              │
              │  Type: TABLE                         │
              │  ────────────────────────────────    │
              │  Differences from V1:                │
              │  • Fewer columns (optimized)         │
              │  • Same aging logic                  │
              │  • Better performance                │
              │  • Simplified for reporting          │
              └──────────────────────────────────────┘
```

---

## 🔗 Join Relationships

### Staging Layer (Layer 2)

**No Joins** - Direct source transformation

```sql
-- STG_AR_INVOICE
SELECT * 
FROM FACT_ACCOUNT_RECEIVABLE_GBL
WHERE clearing_date IS NULL 
  AND account_type = 'D'
  -- ... filters
```

---

### Dimension Layer (Layer 3)

**No Joins** - Direct source transformation

```sql
-- DIM_CUSTOMER
SELECT * FROM CORP_MASTER.DIM_CUSTOMER

-- DIM_FISCAL_CALENDAR
SELECT * FROM CORP_REF.TIME_FISCAL_DAY
```

---

### Mart Layer (Layer 4)

**Two Left Joins** - Enriching AR data with customer and fiscal calendar

```sql
-- DM_FIN_AR_AGING_SIMPLE & DM_FIN_AR_AGING_SIMPLE_V2

FROM stg_ar_invoice ar

LEFT JOIN dim_customer cust
  ON ar.customer_sk = cust.customer_num_sk
  AND ar.source_system = cust.source_system
  
LEFT JOIN dim_fiscal_calendar fc
  ON to_char(ar.posting_date, 'YYYYMMDD') = fc.fiscal_day_key_str
```

**Join Type:** LEFT JOIN (preserves all AR records even if customer/calendar not found)

**Join Keys:**
- **Customer Join:** `customer_sk` + `source_system` (composite key)
- **Calendar Join:** `posting_date` (formatted as YYYYMMDD)

---

## 📊 Data Model Details

### Layer 2: Staging Layer

#### STG_AR_INVOICE

| Attribute | Details |
|-----------|---------|
| **Project** | dbt_foundation |
| **Schema** | EDW.DEV_DBT_DBT_STAGING |
| **Materialization** | VIEW |
| **Source** | CORP_TRAN.FACT_ACCOUNT_RECEIVABLE_GBL |
| **Grain** | One row per open AR invoice line item |
| **Row Count** | ~500 (sample data) |
| **Refresh** | On-demand (dbt build) |

**Filters Applied:**
1. `clearing_date IS NULL` - Only open items
2. `account_type = 'D'` - Only debits (receivables)
3. `special_gl_indicator NOT IN ('A', 'F', 'G')` - Exclude special items
4. `source_system IN (...)` - Only configured source systems

**Key Transformations:**
- `accounting_doc` → `document_number`
- `sold_to` → `customer_number`
- `amt_usd_me` → preserved (amount in USD at month-end rate)
- Added `_stg_loaded_at` timestamp

**Tests:**
- Row count > 0
- Uniqueness of composite key
- Recent data check (within 2 days)

---

### Layer 3: Dimension Layer

#### DIM_CUSTOMER

| Attribute | Details |
|-----------|---------|
| **Project** | dbt_foundation |
| **Schema** | EDW.DEV_DBT_DBT_SHARED |
| **Materialization** | TABLE |
| **Source** | CORP_MASTER.DIM_CUSTOMER |
| **Grain** | One row per customer per source system |
| **Row Count** | ~100 (sample data) |
| **SCD Type** | Type 1 (overwrite) |

**Key Transformations:**
- `customer_num_sk || '|' || source_system` → `customer_id` (composite key)
- `mdm_customer_duns_num` → `duns_number`
- Added `is_internal` flag:
  ```sql
  CASE
    WHEN customer_type = 'I' THEN TRUE
    WHEN customer_name LIKE '%HONEYWELL%' THEN TRUE
    WHEN customer_name LIKE '%ECLIPSE%' THEN TRUE
    WHEN customer_name LIKE '%ELSTER%' THEN TRUE
    ELSE FALSE
  END
  ```

**Tests:**
- Unique combination: `customer_num_sk` + `source_system`
- Not null on key fields
- Accepted values for `customer_type` (E/I)
- Row count > 0

---

#### DIM_FISCAL_CALENDAR

| Attribute | Details |
|-----------|---------|
| **Project** | dbt_foundation |
| **Schema** | EDW.DEV_DBT_DBT_SHARED |
| **Materialization** | TABLE |
| **Source** | CORP_REF.TIME_FISCAL_DAY |
| **Grain** | One row per calendar day |
| **Row Count** | 730 (2 years: 2024-2025) |
| **Date Range** | 2024-01-01 to 2025-12-31 |

**Key Transformations:**
- `fiscal_date_key_date` → `fiscal_date`
- Preserves all fiscal period fields
- Added `_loaded_at` timestamp

**Tests:**
- Unique `fiscal_day_key_str`
- Not null on date fields
- Table row count > 0

---

### Layer 4: Mart Layer

#### DM_FIN_AR_AGING_SIMPLE

| Attribute | Details |
|-----------|---------|
| **Project** | dbt_finance_core |
| **Schema** | EDW.DEV_DBT_DBT_FINANCE |
| **Materialization** | TABLE |
| **Sources** | STG_AR_INVOICE, DIM_CUSTOMER, DIM_FISCAL_CALENDAR |
| **Grain** | One row per open AR invoice line item |
| **Business Area** | Finance - Accounts Receivable |
| **Use Case** | AR aging analysis, collections prioritization |

**Calculated Fields:**

1. **Days Late:**
   ```sql
   DATEDIFF('day', net_due_date, CURRENT_DATE())
   ```

2. **Aging Bucket:**
   ```sql
   CASE
     WHEN days_late <= 0 THEN 'CURRENT'
     WHEN days_late BETWEEN 1 AND 30 THEN '1-30'
     WHEN days_late BETWEEN 31 AND 60 THEN '31-60'
     WHEN days_late BETWEEN 61 AND 90 THEN '61-90'
     WHEN days_late BETWEEN 91 AND 120 THEN '91-120'
     WHEN days_late BETWEEN 121 AND 150 THEN '121-150'
     WHEN days_late BETWEEN 151 AND 180 THEN '151-180'
     WHEN days_late BETWEEN 181 AND 360 THEN '181-360'
     ELSE '361+'
   END
   ```

3. **Past Due Flag:**
   ```sql
   CASE WHEN days_late > 0 THEN 'YES' ELSE 'NO' END
   ```

4. **Current Amount:**
   ```sql
   CASE WHEN days_late <= 0 THEN amt_usd_me ELSE 0 END
   ```

5. **Past Due Amount:**
   ```sql
   CASE WHEN days_late > 0 THEN amt_usd_me ELSE 0 END
   ```

**Output Columns:**
- **Snapshot:** snapshot_date (CURRENT_DATE)
- **Keys:** source_system, company_code, document_number, document_line, document_year
- **Customer:** customer_number, customer_sk, customer_name, customer_type, customer_type_flag
- **Amounts:** amt_usd_me, amt_doc, amt_lcl, doc_currency, local_currency
- **Dates:** document_date, posting_date, due_date
- **Aging:** days_late, aging_bucket, past_due_flag, current_amt, past_due_amt
- **Organization:** gl_account, profit_center, sales_organization
- **Terms:** payment_terms, payment_terms_name
- **Analyst:** credit_analyst_name, credit_analyst_id
- **Fiscal:** fiscal_year_period, fiscal_year, fiscal_period
- **Metadata:** loaded_at

**Tests:**
- Not null on key fields
- Unique combination of invoice identifiers
- Recent data (within 1 day)
- Row count > 0

---

#### DM_FIN_AR_AGING_SIMPLE_V2

| Attribute | Details |
|-----------|---------|
| **Differences from V1** | Fewer columns, optimized for performance |
| **All other attributes** | Same as DM_FIN_AR_AGING_SIMPLE |

**Columns Removed (vs V1):**
- document_year
- amt_doc, amt_lcl, doc_currency, local_currency (keeps only amt_usd_me)
- document_date
- gl_account, profit_center, sales_organization
- payment_terms, payment_terms_name
- credit_analyst_name, credit_analyst_id
- fiscal_year, fiscal_period (keeps only fiscal_year_period)

**Use Case:** Lighter weight report for dashboards and quick queries

---

## 📈 Data Volume Estimates

### Sample Data (Development)

| Layer | Object | Row Count |
|-------|--------|-----------|
| **Source** | FACT_ACCOUNT_RECEIVABLE_GBL | 500 |
| **Source** | DIM_CUSTOMER | 100 |
| **Source** | DIM_ENTITY | 8 |
| **Source** | TIME_FISCAL_DAY | 730 |
| **Staging** | STG_AR_INVOICE | ~500 |
| **Dimension** | DIM_CUSTOMER | 100 |
| **Dimension** | DIM_FISCAL_CALENDAR | 730 |
| **Mart** | DM_FIN_AR_AGING_SIMPLE | ~500 |
| **Mart** | DM_FIN_AR_AGING_SIMPLE_V2 | ~500 |

### Production Estimates

| Layer | Object | Estimated Row Count |
|-------|--------|---------------------|
| **Source** | FACT_ACCOUNT_RECEIVABLE_GBL | 10M - 100M |
| **Source** | DIM_CUSTOMER | 50K - 500K |
| **Source** | DIM_ENTITY | 50 - 200 |
| **Source** | TIME_FISCAL_DAY | 3,650 (10 years) |
| **Staging** | STG_AR_INVOICE | 1M - 10M (open items only) |
| **Dimension** | DIM_CUSTOMER | 50K - 500K |
| **Dimension** | DIM_FISCAL_CALENDAR | 3,650 |
| **Mart** | DM_FIN_AR_AGING_SIMPLE | 1M - 10M |
| **Mart** | DM_FIN_AR_AGING_SIMPLE_V2 | 1M - 10M |

---

## 🔄 Dependency Graph

### Project Dependencies

```
dbt_foundation (no dependencies)
    ↓
dbt_finance_core (depends on dbt_foundation)
```

### Model Dependencies

```
FACT_ACCOUNT_RECEIVABLE_GBL
    ↓
STG_AR_INVOICE
    ↓
DM_FIN_AR_AGING_SIMPLE
DM_FIN_AR_AGING_SIMPLE_V2

CORP_MASTER.DIM_CUSTOMER
    ↓
DIM_CUSTOMER
    ↓
DM_FIN_AR_AGING_SIMPLE
DM_FIN_AR_AGING_SIMPLE_V2

CORP_REF.TIME_FISCAL_DAY
    ↓
DIM_FISCAL_CALENDAR
    ↓
DM_FIN_AR_AGING_SIMPLE
DM_FIN_AR_AGING_SIMPLE_V2
```

### Build Order

1. **dbt_foundation** (parallel execution within project):
   - `STG_AR_INVOICE` (staging)
   - `DIM_CUSTOMER` (dimension)
   - `DIM_FISCAL_CALENDAR` (dimension)

2. **dbt_finance_core** (after foundation complete):
   - `DM_FIN_AR_AGING_SIMPLE` (mart)
   - `DM_FIN_AR_AGING_SIMPLE_V2` (mart)

---

## 🎯 Business Use Cases

### 1. AR Aging Analysis

**Primary Mart:** `DM_FIN_AR_AGING_SIMPLE`

**Questions Answered:**
- What is the total AR balance by aging bucket?
- Which customers have the most past-due invoices?
- What is the trend of aging buckets over time?
- How much AR is at risk (90+ days)?

**Sample Query:**
```sql
SELECT 
    aging_bucket,
    COUNT(*) as invoice_count,
    SUM(amt_usd_me) as total_amount_usd,
    AVG(days_late) as avg_days_late
FROM DM_FIN_AR_AGING_SIMPLE
GROUP BY aging_bucket
ORDER BY 
    CASE aging_bucket
        WHEN 'CURRENT' THEN 1
        WHEN '1-30' THEN 2
        WHEN '31-60' THEN 3
        WHEN '61-90' THEN 4
        WHEN '91-120' THEN 5
        WHEN '121-150' THEN 6
        WHEN '151-180' THEN 7
        WHEN '181-360' THEN 8
        ELSE 9
    END;
```

---

### 2. Collections Prioritization

**Primary Mart:** `DM_FIN_AR_AGING_SIMPLE`

**Questions Answered:**
- Which invoices should collections contact first?
- What is the distribution by credit analyst?
- Which payment terms have the worst aging?

**Sample Query:**
```sql
SELECT 
    customer_name,
    credit_analyst_name,
    COUNT(*) as invoice_count,
    SUM(past_due_amt) as total_past_due,
    MAX(days_late) as max_days_late,
    SUM(CASE WHEN aging_bucket IN ('91-120', '121-150', '151-180', '181-360', '361+') 
            THEN amt_usd_me ELSE 0 END) as high_risk_amount
FROM DM_FIN_AR_AGING_SIMPLE
WHERE past_due_flag = 'YES'
GROUP BY customer_name, credit_analyst_name
HAVING total_past_due > 10000
ORDER BY high_risk_amount DESC, max_days_late DESC
LIMIT 50;
```

---

### 3. Internal vs External Customer Analysis

**Primary Mart:** `DM_FIN_AR_AGING_SIMPLE`

**Questions Answered:**
- Do internal customers pay faster than external?
- What is the AR balance split by customer type?

**Sample Query:**
```sql
SELECT 
    customer_type_flag,
    aging_bucket,
    COUNT(*) as invoice_count,
    SUM(amt_usd_me) as total_amount,
    AVG(days_late) as avg_days_late
FROM DM_FIN_AR_AGING_SIMPLE
GROUP BY customer_type_flag, aging_bucket
ORDER BY customer_type_flag, 
    CASE aging_bucket
        WHEN 'CURRENT' THEN 1
        WHEN '1-30' THEN 2
        -- ...
    END;
```

---

### 4. Fiscal Period Trending

**Primary Mart:** `DM_FIN_AR_AGING_SIMPLE`

**Questions Answered:**
- How does AR aging change by fiscal period?
- Are we improving or degrading collections?

**Sample Query:**
```sql
SELECT 
    fiscal_year_period,
    SUM(current_amt) as current_amount,
    SUM(past_due_amt) as past_due_amount,
    ROUND(SUM(past_due_amt) / NULLIF(SUM(amt_usd_me), 0) * 100, 2) as past_due_pct,
    AVG(days_late) as avg_days_late
FROM DM_FIN_AR_AGING_SIMPLE
GROUP BY fiscal_year_period
ORDER BY fiscal_year_period DESC;
```

---

## 🔍 Data Quality & Testing

### Source Data Tests

**FACT_ACCOUNT_RECEIVABLE_GBL:**
- ✓ Not null: `source_system`, `company_code`, `accounting_doc`, `account_doc_line_item`, `amt_usd_me`
- ✓ Accepted values: `source_system`, `account_type`
- ✓ Freshness: warn 24h, error 48h

**DIM_CUSTOMER:**
- ✓ Not null: `customer_num_sk`, `source_system`
- ✓ Accepted values: `customer_type` (E/I)
- ✓ Unique combination: `customer_num_sk` + `source_system`

**TIME_FISCAL_DAY:**
- ✓ Unique: `fiscal_day_key_str`
- ✓ Not null: `fiscal_day_key_str`, `fiscal_date_key_date`, `fiscal_year_period_int`

---

### Staging Layer Tests

**STG_AR_INVOICE:**
- ✓ Unique combination: `source_system` + `company_code` + `document_number` + `document_line` + `document_year`
- ✓ Not null: `source_system`, `document_number`, `customer_number`, `amt_usd_me`
- ✓ Recency: Data within 2 days (`_stg_loaded_at`)
- ✓ Row count > 0

---

### Dimension Layer Tests

**DIM_CUSTOMER:**
- ✓ Unique combination: `customer_num_sk` + `source_system`
- ✓ Not null: `customer_id`, `customer_num_sk`, `source_system`
- ✓ Accepted values: `customer_type` (E/I)
- ✓ Row count > 0
- ✓ Expect table row count between 90-110 (sample data)

**DIM_FISCAL_CALENDAR:**
- ✓ Unique: `fiscal_day_key_str`
- ✓ Not null: `fiscal_day_key_str`, `fiscal_date`
- ✓ Row count > 0
- ✓ Expect table row count between 700-750 (sample data)

---

### Mart Layer Tests

**DM_FIN_AR_AGING_SIMPLE / DM_FIN_AR_AGING_SIMPLE_V2:**
- ✓ Unique combination: `source_system` + `company_code` + `document_number` + `document_line`
- ✓ Not null: Key fields
- ✓ Recency: Data within 1 day (`loaded_at`)
- ✓ Row count > 0
- ✓ Expect table row count between 450-550 (sample data)

---

## 🚀 Execution Guide

### Step 1: Load Sample Source Data

```sql
-- In Snowsight, run:
@LOAD_SAMPLE_SOURCE_DATA.sql

-- Expected results:
-- ✓ 100 customers loaded
-- ✓ 730 fiscal calendar days loaded
-- ✓ 500 AR invoices loaded
```

---

### Step 2: Build dbt_foundation

```bash
# In Snowsight:
# Navigate to: Projects → dbt_foundation
# Click: Build

# Expected outputs:
# ✓ STG_AR_INVOICE (VIEW)
# ✓ DIM_CUSTOMER (TABLE)
# ✓ DIM_FISCAL_CALENDAR (TABLE)
# ✓ All tests pass
```

---

### Step 3: Build dbt_finance_core

```bash
# In Snowsight:
# Navigate to: Projects → dbt_finance_core
# Click: Build

# Expected outputs:
# ✓ DM_FIN_AR_AGING_SIMPLE (TABLE)
# ✓ DM_FIN_AR_AGING_SIMPLE_V2 (TABLE)
# ✓ All tests pass
```

---

### Step 4: Verify Data Flow

```sql
-- Verify source data
SELECT COUNT(*) FROM EDW.CORP_REF.AR_INVOICE_OPEN;          -- 500
SELECT COUNT(*) FROM EDW.CORP_REF.CUSTOMER;                 -- 100
SELECT COUNT(*) FROM EDW.CORP_REF.TIME_FISCAL_DAY;          -- 730

-- Verify staging layer
SELECT COUNT(*) FROM EDW.DEV_DBT_DBT_STAGING.STG_AR_INVOICE; -- ~500

-- Verify dimension layer
SELECT COUNT(*) FROM EDW.DEV_DBT_DBT_SHARED.DIM_CUSTOMER;          -- 100
SELECT COUNT(*) FROM EDW.DEV_DBT_DBT_SHARED.DIM_FISCAL_CALENDAR;   -- 730

-- Verify mart layer
SELECT COUNT(*) FROM EDW.DEV_DBT_DBT_FINANCE.DM_FIN_AR_AGING_SIMPLE;     -- ~500
SELECT COUNT(*) FROM EDW.DEV_DBT_DBT_FINANCE.DM_FIN_AR_AGING_SIMPLE_V2;  -- ~500

-- Verify data quality
SELECT * FROM EDW.DEV_DBT_DBT_FINANCE.DM_FIN_AR_AGING_SIMPLE LIMIT 10;
```

---

## 📊 Performance Considerations

### Materialization Strategy

| Model | Materialization | Reason |
|-------|----------------|--------|
| **STG_AR_INVOICE** | VIEW | Lightweight filtering, source data changes frequently |
| **DIM_CUSTOMER** | TABLE | Reference data, slower changes, lookup performance |
| **DIM_FISCAL_CALENDAR** | TABLE | Static reference data, lookup performance |
| **DM_FIN_AR_AGING_SIMPLE** | TABLE | Complex calculations, final report, query performance |
| **DM_FIN_AR_AGING_SIMPLE_V2** | TABLE | Optimized for dashboard queries |

---

### Query Performance

**Expected Query Times (Sample Data):**
- Staging queries: < 1 second
- Dimension queries: < 1 second
- Mart queries: < 2 seconds

**Expected Query Times (Production - 10M rows):**
- Staging queries: 2-5 seconds
- Dimension queries: < 1 second
- Mart queries: 5-15 seconds (full scan)

**Optimization Opportunities:**
1. Cluster keys on mart tables (by `snapshot_date`, `aging_bucket`)
2. Partition by fiscal period for historical analysis
3. Incremental materialization for daily refreshes
4. Pre-aggregated summary tables for dashboards

---

## 🔄 Refresh Strategy

### Current Implementation (Full Refresh)

```yaml
Frequency: On-demand (manual trigger in Snowsight)
Method: Full rebuild (CREATE OR REPLACE)
Duration: < 1 minute (sample data)
Data Latency: Real-time (as of execution)
```

### Recommended Production Strategy

```yaml
Frequency: Daily at 6 AM (after source system loads)
Method: 
  - Staging: Full refresh (lightweight views)
  - Dimensions: Full refresh or incremental (SCD logic)
  - Marts: Incremental (append daily snapshots)
Duration: 5-15 minutes (10M rows)
Data Latency: T+1 (previous day close)
```

---

## 📝 Metadata & Lineage

### Column-Level Lineage Example

**`DM_FIN_AR_AGING_SIMPLE.customer_name`**

```
Source Path:
  CORP_MASTER.DIM_CUSTOMER.customer_name
    ↓
  DIM_CUSTOMER.customer_name
    ↓
  DM_FIN_AR_AGING_SIMPLE.customer_name
```

**`DM_FIN_AR_AGING_SIMPLE.aging_bucket`**

```
Calculated Field:
  FACT_ACCOUNT_RECEIVABLE_GBL.net_due_date
    ↓
  STG_AR_INVOICE.net_due_date
    ↓
  DM_FIN_AR_AGING_SIMPLE.days_late (DATEDIFF calculation)
    ↓
  DM_FIN_AR_AGING_SIMPLE.aging_bucket (CASE statement)
```

**`DM_FIN_AR_AGING_SIMPLE.fiscal_year_period`**

```
Source Path:
  CORP_REF.TIME_FISCAL_DAY.fiscal_year_period_str
    ↓
  DIM_FISCAL_CALENDAR.fiscal_year_period_str
    ↓
  DM_FIN_AR_AGING_SIMPLE.fiscal_year_period
```

---

## 🎓 Key Learnings

### Design Principles Applied

1. **Layered Architecture:** Clear separation of staging, dimension, and mart layers
2. **Single Responsibility:** Each model has one clear purpose
3. **DRY (Don't Repeat Yourself):** Shared dimensions reused across marts
4. **Testability:** Comprehensive tests at each layer
5. **Performance:** Strategic materialization choices
6. **Maintainability:** Clear naming conventions, documentation

---

### Snowflake Native DBT Considerations

1. **No Hooks:** Can't use `on-run-end` hooks (e.g., dbt_artifacts won't work)
2. **Macro-Only Packages:** Only packages with macros work (no post-hooks)
3. **Cross-Project References:** Use `source()` instead of `ref()` for cross-project
4. **Idempotency:** All models use `CREATE OR REPLACE` for safe re-runs
5. **Query History:** Use Snowflake's native Query History for observability

---

## 📚 Related Documentation

| Document | Purpose |
|----------|---------|
| **START_HERE.md** | Initial setup and quick start |
| **QUICKSTART.md** | Running dbt projects |
| **LOAD_SAMPLE_SOURCE_DATA.sql** | Sample data loading script |
| **CURRENT_REPOSITORY_STRUCTURE.md** | Repository navigation |
| **IMPLEMENTATION_SUMMARY.md** | Technical implementation details |
| **DATA_QUALITY_TESTS_SUMMARY.md** | All data quality tests |
| **SNOWSIGHT_DASHBOARD_QUERIES.md** | Business intelligence queries |

---

## 📞 Support & Maintenance

### Troubleshooting

**Issue:** Mart tables show zero rows

**Solution:** 
1. Verify source data exists
2. Check staging layer filters (might be too restrictive)
3. Verify cross-project dependencies are built in order

**Issue:** Tests failing on freshness

**Solution:**
1. Run `LOAD_SAMPLE_SOURCE_DATA.sql` to refresh timestamps
2. Adjust test thresholds in YAML files

---

### Future Enhancements

See **FUTURE_IMPLEMENTATIONS.md** for:
- Incremental materialization
- Snapshot tables for historical trending
- Additional marts (AP aging, cash flow)
- Advanced monitoring and alerting
- Performance optimization

---

**Document Version:** 1.0  
**Last Updated:** December 1, 2025  
**Maintained By:** PoC Team  
**Status:** ✅ Production Ready

