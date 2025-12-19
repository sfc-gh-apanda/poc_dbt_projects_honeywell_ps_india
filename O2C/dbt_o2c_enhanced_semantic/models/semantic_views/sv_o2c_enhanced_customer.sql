{{
    config(
        materialized='semantic_view',
        persist_docs={'relation': true, 'columns': true}
    )
}}

/*
    O2C Enhanced Customer Summary Semantic View
    
    Enables Cortex Analyst natural language queries about:
    - Customer lifetime value, performance, segmentation, risk analysis
    - Includes audit columns for data freshness queries
    
    Source: EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER
*/

TABLES(
    EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER
)
DIMENSIONS(
    customer_name COMMENT = 'Customer company name' SYNONYMS ('customer', 'company', 'account', 'client'),
    customer_type COMMENT = 'Customer type: E=External, I=Internal' SYNONYMS ('account type', 'customer category'),
    customer_country COMMENT = 'Customer country' SYNONYMS ('country', 'location', 'geography'),
    customer_classification COMMENT = 'Customer classification code' SYNONYMS ('classification', 'class', 'customer class'),
    dbt_environment COMMENT = 'Environment: dev or prod' SYNONYMS ('environment', 'env'),
    dbt_source_model COMMENT = 'Source dbt model' SYNONYMS ('source model', 'model name')
)
FACTS(
    total_order_value COMMENT = 'Customer lifetime order value in USD' SYNONYMS ('lifetime value', 'customer revenue', 'LTV', 'total revenue'),
    total_invoice_value COMMENT = 'Customer lifetime invoice value in USD' SYNONYMS ('total billed', 'lifetime invoices'),
    total_payment_value COMMENT = 'Customer lifetime payment value in USD' SYNONYMS ('total collected', 'lifetime payments', 'total cash'),
    current_ar_outstanding COMMENT = 'Current AR outstanding in USD' SYNONYMS ('customer AR', 'receivables', 'AR balance', 'customer balance'),
    total_order_count COMMENT = 'Total number of orders' SYNONYMS ('order count', 'number of orders', 'order volume'),
    total_invoice_count COMMENT = 'Total number of invoices' SYNONYMS ('invoice count', 'number of invoices'),
    total_payment_count COMMENT = 'Total number of payments' SYNONYMS ('payment count', 'number of payments'),
    avg_days_sales_outstanding COMMENT = 'Average DSO for customer' SYNONYMS ('customer DSO', 'average DSO', 'DSO', 'collection period'),
    avg_days_to_invoice COMMENT = 'Average days to invoice' SYNONYMS ('billing time', 'invoice lag'),
    avg_days_to_payment COMMENT = 'Average days to payment' SYNONYMS ('collection time', 'payment time'),
    first_order_date COMMENT = 'First order date' SYNONYMS ('first order', 'customer since', 'first purchase'),
    last_order_date COMMENT = 'Most recent order date' SYNONYMS ('last order', 'most recent order', 'latest order'),
    dbt_loaded_at COMMENT = 'Last refresh timestamp' SYNONYMS ('refreshed date', 'updated time', 'last refresh'),
    dbt_created_at COMMENT = 'Record creation timestamp' SYNONYMS ('created date', 'creation time'),
    dbt_updated_at COMMENT = 'Last update timestamp' SYNONYMS ('updated date', 'last modified')
)
METRICS(
    customer_lifetime_revenue AS SUM(total_order_value) COMMENT = 'Sum of all customer LTV' SYNONYMS ('total revenue', 'total customer value'),
    avg_customer_lifetime_value AS AVG(total_order_value) COMMENT = 'Average customer LTV' SYNONYMS ('average LTV', 'mean customer value'),
    median_customer_lifetime_value AS MEDIAN(total_order_value) COMMENT = 'Median customer LTV' SYNONYMS ('median LTV'),
    customer_current_ar AS SUM(current_ar_outstanding) COMMENT = 'Total AR across all customers' SYNONYMS ('total AR', 'total receivables'),
    avg_customer_ar AS AVG(current_ar_outstanding) COMMENT = 'Average AR per customer' SYNONYMS ('average AR', 'mean customer AR'),
    customer_collection_rate AS SUM(total_payment_value) / NULLIF(SUM(total_invoice_value), 0) COMMENT = 'Overall collection rate' SYNONYMS ('collection rate', 'payment rate'),
    avg_customer_dso AS AVG(avg_days_sales_outstanding) COMMENT = 'Average DSO across customers' SYNONYMS ('average DSO', 'mean DSO'),
    median_customer_dso AS MEDIAN(avg_days_sales_outstanding) COMMENT = 'Median customer DSO' SYNONYMS ('median DSO'),
    total_customers AS COUNT(DISTINCT customer_name) COMMENT = 'Total unique customers' SYNONYMS ('customer count', 'number of customers'),
    active_customers AS COUNT_IF(total_order_count > 0) COMMENT = 'Customers with orders' SYNONYMS ('active customer count'),
    high_value_customers AS COUNT_IF(total_order_value > 100000) COMMENT = 'Customers with LTV over $100K' SYNONYMS ('high value count', 'premium customers'),
    at_risk_customers AS COUNT_IF(avg_days_sales_outstanding > 60) COMMENT = 'Customers with DSO over 60 days' SYNONYMS ('high DSO customers', 'slow pay customers'),
    customers_with_ar AS COUNT_IF(current_ar_outstanding > 0) COMMENT = 'Customers with outstanding AR' SYNONYMS ('AR customers', 'open balance customers'),
    high_risk_customers AS COUNT_IF(avg_days_sales_outstanding > 90) COMMENT = 'Customers with DSO over 90 days' SYNONYMS ('very slow pay customers', 'high risk count'),
    high_value_at_risk AS COUNT_IF(total_order_value > 100000 AND avg_days_sales_outstanding > 60) COMMENT = 'High-value customers at risk' SYNONYMS ('premium customers at risk'),
    countries_served AS COUNT(DISTINCT customer_country) COMMENT = 'Number of unique countries' SYNONYMS ('country count', 'geographic coverage'),
    customer_records AS COUNT(*) COMMENT = 'Total customer records' SYNONYMS ('record count', 'row count'),
    customers_updated_today AS COUNT_IF(DATE(dbt_updated_at) = CURRENT_DATE()) COMMENT = 'Customers updated today' SYNONYMS ('today updates', 'daily updates')
)
COMMENT = 'Enhanced customer summary semantic view with lifetime metrics and risk analysis for Cortex Analyst'
