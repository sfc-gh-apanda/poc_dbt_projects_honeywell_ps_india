# dbt_o2c_enhanced_semantic

**Semantic Views for Enhanced O2C Analytics with Cortex Analyst**

---

## 📊 Overview

This project creates **Snowflake SEMANTIC VIEW** objects that enable **Cortex Analyst** to answer natural language questions about your O2C data.

**Key Features:**
- ✅ **persist_docs enabled** - Documentation automatically synced to Snowflake
- ✅ **Extensive synonyms** - 100+ synonyms for natural language understanding
- ✅ **Audit columns exposed** - Data quality and lineage queries via natural language
- ✅ **25+ business metrics** - Pre-calculated KPIs for revenue, AR, DSO, risk
- ✅ **Two semantic views** - Reconciliation (transaction-level) and Customer (aggregate-level)

**Package:** [Snowflake-Labs/dbt_semantic_view v1.0.3](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view/latest/)

---

## 🎯 What's Included

### 1. **sv_o2c_enhanced_reconciliation** - Transaction-Level Semantic View

**Source:** `dm_o2c_reconciliation` (enhanced with audit columns)

**Features:**
- 12 dimensions (customer, status, dates, audit metadata)
- 14 facts (amounts, cycle times, audit timestamps, row hash)
- 25+ metrics (revenue, AR, DSO, collection rates, data quality)
- 100+ synonyms for natural language queries

**Key Dimensions:**
| Dimension | Description | Synonyms |
|-----------|-------------|----------|
| `customer_name` | Customer company name | customer, company, account, client |
| `customer_type` | External vs Internal | account type, customer category |
| `reconciliation_status` | O2C pipeline status | status, state, O2C status |
| `payment_timing` | Payment timeliness | payment status, timeliness |
| `dbt_environment` | dev or prod | environment, env |

**Key Metrics:**
| Metric | Description | Formula |
|--------|-------------|---------|
| `total_revenue` | Total order value | SUM(order_amount) |
| `total_ar_outstanding` | Total receivables | SUM(outstanding_amount) |
| `avg_dso` | Average DSO | AVG(days_order_to_cash) |
| `collection_rate` | Collection efficiency | SUM(payment) / SUM(invoice) |
| `overdue_ar_amount` | Overdue receivables | SUM(outstanding WHERE past_due > 0) |

### 2. **sv_o2c_enhanced_customer** - Customer-Level Semantic View

**Source:** `agg_o2c_by_customer` (enhanced with audit columns)

**Features:**
- 6 dimensions (customer attributes, audit metadata)
- 13 facts (lifetime metrics, performance, timeline)
- 20+ metrics (LTV, AR, DSO, segmentation, risk)
- Customer segmentation and risk scoring

**Key Dimensions:**
| Dimension | Description | Synonyms |
|-----------|-------------|----------|
| `customer_name` | Customer identifier | customer, company, account |
| `customer_country` | Geographic location | country, location, geography |
| `customer_type` | Customer classification | account type, customer category |

**Key Metrics:**
| Metric | Description | Formula |
|--------|-------------|---------|
| `customer_lifetime_revenue` | Total customer value | SUM(total_order_value) |
| `customer_current_ar` | Total customer AR | SUM(current_ar_outstanding) |
| `high_value_customers` | Customers >$100K | COUNT_IF(LTV > 100000) |
| `at_risk_customers` | High DSO customers | COUNT_IF(DSO > 60) |
| `high_value_at_risk` | Premium customers at risk | COUNT_IF(LTV > 100K AND DSO > 60) |

---

## 🚀 Deployment

### Step 0: Create Schema (One-Time Setup)

**Run this ONCE in Snowflake before first deployment:**

```sql
-- In Snowflake worksheet, run:
CREATE SCHEMA IF NOT EXISTS EDW.O2C_ENHANCED_SEMANTIC_VIEWS
    COMMENT = 'Semantic views for Cortex Analyst';

GRANT ALL ON SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS TO ROLE DBT_O2C_DEVELOPER;
```

Or use the provided script:
```bash
snowsql -f SETUP_SCHEMA.sql
```

### Step 1: Install Dependencies

```bash
cd dbt_o2c_enhanced_semantic

# Install the semantic view package
dbt deps

# Expected output:
# Installing Snowflake-Labs/dbt_semantic_view@1.0.3
# Installed 1 package
```

### Step 2: Build Semantic Views

```bash
# Create semantic view objects in Snowflake
dbt build

# Expected output:
# Running 2 semantic_view models
# ✓ sv_o2c_enhanced_reconciliation ... SUCCESS [CREATE SEMANTIC VIEW]
# ✓ sv_o2c_enhanced_customer .......... SUCCESS [CREATE SEMANTIC VIEW]
# 
# Completed successfully in 2.3s
```

### Step 3: Verify Deployment

```sql
-- Check semantic views were created
SHOW SEMANTIC VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS;

-- View semantic view definition
DESCRIBE SEMANTIC VIEW EDW.O2C_ENHANCED_SEMANTIC_VIEWS.SV_O2C_ENHANCED_RECONCILIATION;

-- View documentation (persist_docs feature)
SELECT 
    table_name,
    comment
FROM EDW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'O2C_ENHANCED_SEMANTIC_VIEWS'
  AND table_type = 'SEMANTIC VIEW';
```

---

## 💡 Example Natural Language Queries

Once deployed, you can ask Cortex Analyst these questions:

### Revenue & Orders

**Question:** "What is the total order value by customer type?"

**Cortex Analyst generates:**
```sql
SELECT 
    customer_type,
    SUM(order_amount) as total_order_value
FROM sv_o2c_enhanced_reconciliation
GROUP BY customer_type;
```

**Question:** "Show me top 10 customers by revenue"

**Cortex Analyst generates:**
```sql
SELECT 
    customer_name,
    SUM(order_amount) as total_revenue
FROM sv_o2c_enhanced_reconciliation
GROUP BY customer_name
ORDER BY total_revenue DESC
LIMIT 10;
```

### AR & Collections

**Question:** "How much AR is outstanding for external customers in Germany?"

**Cortex Analyst generates:**
```sql
SELECT 
    SUM(outstanding_amount) as total_ar
FROM sv_o2c_enhanced_reconciliation
WHERE customer_type = 'E'
  AND customer_country = 'Germany';
```

**Question:** "Which customers have AR over $50,000?"

**Cortex Analyst generates:**
```sql
SELECT 
    customer_name,
    SUM(outstanding_amount) as total_ar
FROM sv_o2c_enhanced_reconciliation
GROUP BY customer_name
HAVING SUM(outstanding_amount) > 50000
ORDER BY total_ar DESC;
```

### Performance & DSO

**Question:** "What's the average DSO for external customers?"

**Cortex Analyst generates:**
```sql
SELECT 
    AVG(days_order_to_cash) as avg_dso
FROM sv_o2c_enhanced_reconciliation
WHERE customer_type = 'E';
```

**Question:** "Show me customers with DSO over 60 days"

**Cortex Analyst generates:**
```sql
SELECT 
    customer_name,
    avg_days_sales_outstanding as dso
FROM sv_o2c_enhanced_customer
WHERE avg_days_sales_outstanding > 60
ORDER BY dso DESC;
```

### Data Quality (Enhanced Feature)

**Question:** "Show me records that were updated in the last 24 hours"

**Cortex Analyst generates:**
```sql
SELECT 
    customer_name,
    order_amount,
    dbt_updated_at
FROM sv_o2c_enhanced_reconciliation
WHERE dbt_updated_at > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY dbt_updated_at DESC;
```

**Question:** "Show me production data only"

**Cortex Analyst generates:**
```sql
SELECT *
FROM sv_o2c_enhanced_reconciliation
WHERE dbt_environment = 'prod';
```

### Customer Segmentation

**Question:** "How many high-value customers are at risk?"

**Cortex Analyst generates:**
```sql
SELECT 
    COUNT(*) as high_value_at_risk
FROM sv_o2c_enhanced_customer
WHERE total_order_value > 100000
  AND avg_days_sales_outstanding > 60;
```

**Question:** "Show me the collection rate by customer country"

**Cortex Analyst generates:**
```sql
SELECT 
    customer_country,
    SUM(total_payment_value) / NULLIF(SUM(total_invoice_value), 0) as collection_rate
FROM sv_o2c_enhanced_customer
GROUP BY customer_country
ORDER BY collection_rate DESC;
```

---

## 📋 persist_docs Feature

With `persist_docs` enabled, all documentation from YAML files is automatically synced to Snowflake:

```sql
-- View relation-level comments
SELECT 
    table_name,
    comment
FROM EDW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'O2C_ENHANCED_SEMANTIC_VIEWS';

-- View column-level comments
SELECT 
    table_name,
    column_name,
    comment
FROM EDW.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'O2C_ENHANCED_SEMANTIC_VIEWS'
  AND comment IS NOT NULL
ORDER BY table_name, ordinal_position;
```

**Benefits:**
- ✅ Documentation visible in Snowflake UI
- ✅ Self-documenting data catalog
- ✅ Business users can understand data without dbt access
- ✅ Cortex Analyst uses comments for better understanding

---

## 🔍 Verification & Testing

### Check Semantic Views Created

```sql
-- List all semantic views
SHOW SEMANTIC VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS;

-- Output should show:
-- name: SV_O2C_ENHANCED_RECONCILIATION
-- name: SV_O2C_ENHANCED_CUSTOMER
```

### View Semantic View Definition

```sql
-- See full semantic view structure
DESCRIBE SEMANTIC VIEW EDW.O2C_ENHANCED_SEMANTIC_VIEWS.SV_O2C_ENHANCED_RECONCILIATION;

-- Shows all dimensions, facts, metrics, and synonyms
```

### Test Manual SQL Query

```sql
-- Test reconciliation view
SELECT 
    customer_name,
    total_order_value,
    avg_days_sales_outstanding
FROM EDW.O2C_ENHANCED_SEMANTIC_VIEWS.SV_O2C_ENHANCED_RECONCILIATION
WHERE customer_type = 'E'
LIMIT 10;

-- Test customer view
SELECT 
    customer_name,
    total_order_value as lifetime_revenue,
    current_ar_outstanding as customer_ar,
    avg_days_sales_outstanding as customer_dso
FROM EDW.O2C_ENHANCED_SEMANTIC_VIEWS.SV_O2C_ENHANCED_CUSTOMER
ORDER BY total_order_value DESC
LIMIT 10;
```

### Test Cortex Analyst Integration

```sql
-- In Snowsight, navigate to:
-- Projects > Cortex Analyst

-- Select semantic views:
-- - SV_O2C_ENHANCED_RECONCILIATION
-- - SV_O2C_ENHANCED_CUSTOMER

-- Ask a test question:
-- "What is the total order value?"

-- Verify Cortex Analyst generates and executes SQL
```

---

## 🏗️ Architecture

### Data Flow

```
dbt_o2c_enhanced (Data Transformation)
  ├── dm_o2c_reconciliation (TABLE)
  │   └── Enhanced with audit columns
  │
  └── agg_o2c_by_customer (TABLE)
      └── Enhanced with audit columns
            ↓
dbt_o2c_enhanced_semantic (Metadata Layer)
  ├── sv_o2c_enhanced_reconciliation (SEMANTIC VIEW)
  │   └── Exposes dimensions, facts, metrics, synonyms
  │
  └── sv_o2c_enhanced_customer (SEMANTIC VIEW)
      └── Exposes dimensions, facts, metrics, synonyms
            ↓
Cortex Analyst (AI Query Engine)
  └── Reads semantic view definitions
  └── Understands business meaning
  └── Generates SQL from natural language
            ↓
Natural Language Query → SQL → Results
```

### Key Concepts

**Semantic Views:**
- Snowflake database objects (not just metadata)
- Define business meaning (dimensions, facts, metrics)
- Enable AI to understand your data
- Queried like regular views (also via natural language)

**Dimensions:**
- Attributes for filtering and grouping
- Examples: customer_name, country, status, dates
- Support synonyms for natural language

**Facts:**
- Measurable numeric values
- Examples: amounts, cycle times, counts
- Can be aggregated in metrics

**Metrics:**
- Pre-calculated KPIs using facts
- Examples: SUM(amount), AVG(dso), COUNT(*)
- Simplify common business questions

---

## 🎓 Low-Hanging Features Implemented

### 1. ✅ persist_docs Enabled

```yaml
# dbt_project.yml
+persist_docs:
  relation: true
  columns: true
```

**Benefits:**
- Documentation synced to Snowflake automatically
- Visible in Snowflake UI without dbt
- Self-documenting data catalog

### 2. ✅ Column-Level Comments

```yaml
# _semantic_views.yml
columns:
  - name: customer_name
    description: "Customer company name - primary identifier"
  - name: outstanding_amount
    description: "AR outstanding in USD (invoice - payment)"
```

**Benefits:**
- Clear column definitions
- Helps Cortex Analyst understand context
- Improves data literacy

### 3. ✅ Extensive Synonyms (100+)

```sql
customer_name SYNONYMS ('customer', 'company', 'account', 'client')
days_order_to_cash SYNONYMS ('DSO', 'days sales outstanding', 'collection period', 'cycle time')
```

**Benefits:**
- Natural language flexibility
- Handles different business terminology
- Better Cortex Analyst understanding

### 4. ✅ Audit Columns Exposed

```sql
DIMENSIONS(
    dbt_environment,
    dbt_source_model
)
FACTS(
    dbt_created_at,
    dbt_updated_at,
    dbt_row_hash
)
```

**Benefits:**
- Data quality queries via natural language
- "Show me records updated today"
- "Filter to production data only"
- Data lineage and freshness checks

### 5. ✅ Business Metrics Pre-Calculated

```sql
METRICS(
    total_revenue AS SUM(order_amount),
    avg_dso AS AVG(days_order_to_cash),
    collection_rate AS SUM(payment) / SUM(invoice),
    high_value_customers AS COUNT_IF(ltv > 100000)
)
```

**Benefits:**
- Consistent metric definitions
- Single source of truth
- Faster query execution

### 6. ✅ Risk Scoring & Segmentation

```sql
at_risk_customers AS COUNT_IF(dso > 60),
high_value_at_risk AS COUNT_IF(ltv > 100K AND dso > 60),
overdue_ar_amount AS SUM(CASE WHEN past_due > 0 THEN ar END)
```

**Benefits:**
- Business intelligence built-in
- Customer segmentation ready
- Risk monitoring enabled

---

## 🔗 References

- [Snowflake Engineering Blog: dbt Semantic View Package](https://www.snowflake.com/en/engineering-blog/dbt-semantic-view-package/)
- [dbt Hub: Snowflake-Labs/dbt_semantic_view](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view/latest/)
- [Snowflake Semantic Views Documentation](https://docs.snowflake.com/en/user-guide/semantic-views)
- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/ml-powered-analysis)

---

## 📦 Project Structure

```
dbt_o2c_enhanced_semantic/
├── dbt_project.yml              # Project config with persist_docs
├── packages.yml                 # dbt_semantic_view package
├── profiles.yml                 # Snowflake connection
├── README.md                    # This file
│
└── models/
    └── semantic_views/
        ├── _semantic_views.yml  # Documentation (synced to Snowflake)
        ├── sv_o2c_enhanced_reconciliation.sql  # Transaction-level view
        └── sv_o2c_enhanced_customer.sql        # Customer-level view
```

---

## 🚦 Next Steps

1. ✅ **Deploy semantic views:** `dbt build`
2. ✅ **Verify in Snowflake:** `SHOW SEMANTIC VIEWS`
3. ✅ **Test Cortex Analyst:** Ask natural language questions
4. ✅ **Train business users:** Share example questions
5. ✅ **Monitor usage:** Track which questions are asked most
6. ✅ **Iterate:** Add more synonyms based on user feedback

---

## 💬 Example Questions Cheat Sheet

**Revenue:**
- What is the total revenue?
- Show me revenue by customer type
- Which customers have the highest sales?

**AR & Collections:**
- How much AR is outstanding?
- Show me overdue invoices
- What's the collection rate?

**Performance:**
- What is the average DSO?
- Which customers pay slowly?
- Show me cycle time trends

**Segmentation:**
- How many high-value customers do we have?
- Which customers are at risk?
- Show me active customers by country

**Data Quality:**
- Show me records updated today
- Filter to production data
- Which records were created this week?

---

**Status:** ✅ Production-Ready

**Maintained by:** Data Engineering Team

**Last Updated:** December 2024

