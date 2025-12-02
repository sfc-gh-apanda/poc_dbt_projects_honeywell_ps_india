# dbt_o2c_semantic - O2C Semantic Views for Cortex Analyst

**Project Type:** Snowflake Semantic Views (YAML definitions)  
**Purpose:** Enable Cortex Analyst to answer natural language questions  
**Package:** [Snowflake-Labs/dbt_semantic_view v1.0.3](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view/latest/)  
**Data Flow:** Creates semantic view definitions (metadata), no tables created

---

## 📊 Overview

This project uses **Snowflake's dbt_semantic_view package** to create **semantic view definitions** that enable **Cortex Analyst** to understand your O2C data and answer natural language business questions.

**What are Semantic Views?**
- YAML definitions that describe business meaning of your data
- Enable AI (Cortex Analyst) to generate SQL from natural language
- No database objects created - just metadata

---

## 🎯 What's Included

**2 Semantic View Definitions:**

1. **`o2c_reconciliation_semantic`** - Complete O2C reconciliation data
   - Source: `dm_o2c_reconciliation`
   - 10 dimensions (customer, status, dates, etc.)
   - 11 measures (orders, revenue, DSO, AR, etc.)
   - Synonyms for natural language queries

2. **`o2c_customer_metrics_semantic`** - Customer-level aggregates
   - Source: `agg_o2c_by_customer`
   - 4 dimensions (customer attributes)
   - 6 measures (customer lifetime metrics)

---

## 🚀 Deploy

### **Method 1: Using SQL Script (Recommended)**

```bash
# Run the deployment script in Snowflake
snowsql -f ../O2C_DEPLOY_SEMANTIC_VIEWS.sql
```

This creates Snowflake `SEMANTIC VIEW` objects that Cortex Analyst can query.

### **Method 2: Manual Deployment**

Copy the SQL from `O2C_DEPLOY_SEMANTIC_VIEWS.sql` and run in Snowsight worksheet.

**Note:** The `dbt_semantic_view` package provides YAML schema for semantic views. The actual deployment uses Snowflake SQL `CREATE SEMANTIC VIEW` statements.

---

## 🤖 Query with Cortex Analyst

Once deployed, you can ask Cortex Analyst natural language questions:

### **Example Questions:**

**Revenue & Orders:**
- "What is the total order value by customer type?"
- "Show me top 10 customers by revenue"
- "What's the billing rate for external customers?"

**Collections & AR:**
- "How much AR is outstanding for each country?"
- "Which customers have the highest overdue amount?"
- "What's the collection rate by customer type?"

**Performance:**
- "What is the average DSO by customer?"
- "Show me customers with DSO over 60 days"
- "What's the average time from order to invoice?"

**Cortex Analyst** will automatically:
1. Understand the question using semantic view definitions
2. Generate appropriate SQL
3. Query the underlying `dm_o2c_reconciliation` table
4. Return results

---

## 📋 Semantic View Features

### **Dimensions** (for filtering & grouping):
- `customer_name` (synonyms: customer, company, account)
- `customer_type` (External vs Internal)
- `customer_country` (synonyms: country, location)
- `reconciliation_status` (NOT_INVOICED, NOT_PAID, OPEN, CLOSED)
- `payment_timing` (ON_TIME, LATE, OVERDUE, CURRENT)
- Time dimensions: `order_date`, `invoice_date`, `payment_date`

### **Measures** (for aggregation):
- `total_orders` (synonyms: order count, number of orders)
- `total_order_value` (synonyms: revenue, sales)
- `total_ar_outstanding` (synonyms: receivables, unpaid invoices)
- `avg_days_sales_outstanding` (synonyms: DSO, collection period)
- `avg_days_to_invoice` (synonyms: billing time)
- `avg_days_to_payment` (synonyms: collection time)
- And more...

---

## 🔍 Verify Deployment

```sql
-- Check if semantic views were created
SHOW SEMANTIC VIEWS IN SCHEMA EDW.O2C_SEMANTIC_VIEWS;

-- View semantic view definition
DESCRIBE SEMANTIC VIEW EDW.O2C_SEMANTIC_VIEWS.O2C_RECONCILIATION_SEMANTIC;

-- Test query (manual SQL)
SELECT customer_name, total_order_value, avg_days_sales_outstanding
FROM EDW.O2C_SEMANTIC_VIEWS.O2C_RECONCILIATION_SEMANTIC
WHERE customer_type = 'E';
```

---

## 💡 How It Works

```
1. You define semantic views in YAML
   └─> Dimensions, measures, synonyms

2. dbt_semantic_view package creates Snowflake SEMANTIC VIEW objects
   └─> Stored as metadata in Snowflake

3. Cortex Analyst reads semantic view definitions
   └─> Understands business meaning

4. User asks natural language question
   └─> "What's the DSO for external customers?"

5. Cortex Analyst generates SQL
   └─> SELECT customer_type, AVG(days_order_to_cash)...

6. SQL executes against underlying table
   └─> dm_o2c_reconciliation

7. Results returned to user
   └─> Interactive data exploration
```

---

## 📝 Key Differences

| Aspect | Semantic Views (This) | dbt Semantic Layer (MetricFlow) | DYNAMIC TABLEs |
|--------|----------------------|--------------------------------|----------------|
| **Purpose** | Cortex Analyst interface | dbt Cloud metrics API | Auto-refreshing tables |
| **Creates Objects** | Yes (SEMANTIC VIEW) | No (metadata only) | Yes (DYNAMIC TABLE) |
| **Query Method** | Natural language → SQL | Metrics API | Standard SQL |
| **AI Integration** | ✅ Yes (Cortex Analyst) | ❌ No | ❌ No |
| **Package** | Snowflake-Labs/dbt_semantic_view | dbt-labs/dbt_semantic_interfaces | Native Snowflake |

---

## 🎓 Example Cortex Analyst Conversation

**User:** "Show me customers with outstanding AR over $10,000"

**Cortex Analyst:** 
```sql
SELECT 
    customer_name,
    total_ar_outstanding,
    customer_country,
    avg_days_sales_outstanding
FROM o2c_reconciliation_semantic
WHERE total_ar_outstanding > 10000
ORDER BY total_ar_outstanding DESC;
```

**User:** "What's the average DSO for German customers?"

**Cortex Analyst:**
```sql
SELECT 
    customer_country,
    AVG(avg_days_sales_outstanding) as avg_dso
FROM o2c_reconciliation_semantic
WHERE customer_country = 'Germany'
GROUP BY customer_country;
```

---

## 🔗 References

- [Snowflake Semantic Views Documentation](https://docs.snowflake.com/en/user-guide/semantic-views)
- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/ml-powered-analysis)
- [dbt_semantic_view Package](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view/latest/)

---

For more details, see `O2C_README.md` in the parent folder.
