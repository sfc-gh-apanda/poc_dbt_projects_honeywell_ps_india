{{
    config(
        materialized='semantic_view',
        persist_docs={'relation': true, 'columns': true}
    )
}}

/*
    O2C Enhanced Customer Summary Semantic View
    
    Purpose: Enable Cortex Analyst to answer natural language questions about:
    - Customer lifetime value and performance
    - Customer segmentation and risk analysis
    - AR aging by customer
    - Customer data quality tracking
    
    Source: agg_o2c_by_customer (Enhanced with audit columns)
    
    Materialization: Snowflake SEMANTIC VIEW (using dbt_semantic_view package)
    
    Example Questions:
    - "Show me top 10 customers by revenue"
    - "Which customers have DSO over 60 days?"
    - "What's the total AR for external customers in Germany?"
    - "Show me high-value customers with overdue payments"
*/

TABLES(
    {{ ref('dbt_o2c_enhanced', 'agg_o2c_by_customer') }}
)

DIMENSIONS(
    -- ═══════════════════════════════════════════════════════════════
    -- CUSTOMER DIMENSIONS
    -- ═══════════════════════════════════════════════════════════════
    
    customer_name 
        COMMENT = 'Customer company name' 
        SYNONYMS ('customer', 'company', 'account', 'client', 'organization', 'business'),
    
    customer_type 
        COMMENT = 'Customer type: E=External, I=Internal' 
        SYNONYMS ('account type', 'customer category', 'customer segment', 'customer class', 'account category'),
    
    customer_country 
        COMMENT = 'Customer country location' 
        SYNONYMS ('country', 'location', 'geography', 'nation', 'region', 'market'),
    
    customer_classification 
        COMMENT = 'Customer classification code' 
        SYNONYMS ('classification', 'class', 'customer class', 'account classification'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT & DATA QUALITY DIMENSIONS (Enhanced Feature)
    -- ═══════════════════════════════════════════════════════════════
    
    dbt_environment 
        COMMENT = 'Data environment: dev or prod'
        SYNONYMS ('environment', 'env', 'data environment'),
    
    dbt_source_model 
        COMMENT = 'Source dbt model that created this aggregation'
        SYNONYMS ('source model', 'dbt model', 'model name')
)

FACTS(
    -- ═══════════════════════════════════════════════════════════════
    -- LIFETIME VALUE FACTS (Amounts)
    -- ═══════════════════════════════════════════════════════════════
    
    total_order_value 
        COMMENT = 'Customer lifetime order value in USD' 
        SYNONYMS ('lifetime value', 'customer revenue', 'total revenue', 'LTV', 'customer value', 'total sales'),
    
    total_invoice_value 
        COMMENT = 'Customer lifetime invoice value in USD' 
        SYNONYMS ('total billed', 'lifetime invoices', 'total billing', 'billed amount'),
    
    total_payment_value 
        COMMENT = 'Customer lifetime payment value in USD' 
        SYNONYMS ('total collected', 'lifetime payments', 'cash collected', 'total cash'),
    
    current_ar_outstanding 
        COMMENT = 'Current AR outstanding for customer in USD' 
        SYNONYMS ('customer AR', 'receivables', 'open AR', 'outstanding balance', 'AR balance', 'customer balance'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- VOLUME FACTS (Counts)
    -- ═══════════════════════════════════════════════════════════════
    
    total_order_count 
        COMMENT = 'Total number of orders from this customer' 
        SYNONYMS ('order count', 'number of orders', 'order volume', 'order quantity', 'orders placed'),
    
    total_invoice_count 
        COMMENT = 'Total number of invoices for this customer' 
        SYNONYMS ('invoice count', 'number of invoices', 'invoice volume', 'invoices generated'),
    
    total_payment_count 
        COMMENT = 'Total number of payments from this customer' 
        SYNONYMS ('payment count', 'number of payments', 'payment volume', 'payments made'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- PERFORMANCE FACTS (Days)
    -- ═══════════════════════════════════════════════════════════════
    
    avg_days_sales_outstanding 
        COMMENT = 'Average DSO for this customer' 
        SYNONYMS ('customer DSO', 'average DSO', 'DSO', 'collection period', 'customer collection days'),
    
    avg_days_to_invoice 
        COMMENT = 'Average days from order to invoice for this customer' 
        SYNONYMS ('billing time', 'invoice lag', 'time to invoice', 'billing lag'),
    
    avg_days_to_payment 
        COMMENT = 'Average days from invoice to payment for this customer' 
        SYNONYMS ('collection time', 'payment time', 'time to collect', 'payment lag'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- TIMELINE FACTS
    -- ═══════════════════════════════════════════════════════════════
    
    first_order_date 
        COMMENT = 'Date of first order from this customer' 
        SYNONYMS ('first order', 'customer since', 'first purchase', 'acquisition date', 'onboarding date'),
    
    last_order_date 
        COMMENT = 'Date of most recent order from this customer' 
        SYNONYMS ('last order', 'most recent order', 'latest order', 'last purchase'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT & TRACKING FACTS (Enhanced Feature)
    -- ═══════════════════════════════════════════════════════════════
    
    dbt_loaded_at 
        COMMENT = 'Timestamp when this customer summary was last refreshed'
        SYNONYMS ('refreshed date', 'updated time', 'last refresh', 'refresh timestamp'),
    
    dbt_created_at 
        COMMENT = 'Timestamp when this customer record was first created'
        SYNONYMS ('created date', 'creation time', 'first seen'),
    
    dbt_updated_at 
        COMMENT = 'Timestamp when this customer record was last updated'
        SYNONYMS ('updated date', 'last modified', 'modification time')
)

METRICS(
    -- ═══════════════════════════════════════════════════════════════
    -- REVENUE METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    customer_lifetime_revenue 
        AS SUM(total_order_value) 
        COMMENT = 'Sum of all customer lifetime values' 
        SYNONYMS ('total revenue', 'total customer value', 'aggregate LTV', 'total sales'),
    
    avg_customer_lifetime_value 
        AS AVG(total_order_value) 
        COMMENT = 'Average customer lifetime value' 
        SYNONYMS ('average LTV', 'mean customer value', 'avg customer revenue'),
    
    median_customer_lifetime_value 
        AS MEDIAN(total_order_value) 
        COMMENT = 'Median customer lifetime value' 
        SYNONYMS ('median LTV', '50th percentile customer value'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- AR & COLLECTIONS METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    customer_current_ar 
        AS SUM(current_ar_outstanding) 
        COMMENT = 'Total AR outstanding across all customers' 
        SYNONYMS ('total AR', 'total receivables', 'aggregate AR', 'outstanding AR total'),
    
    avg_customer_ar 
        AS AVG(current_ar_outstanding) 
        COMMENT = 'Average AR outstanding per customer' 
        SYNONYMS ('average AR', 'mean customer AR', 'avg receivables per customer'),
    
    customer_collection_rate 
        AS SUM(total_payment_value) / NULLIF(SUM(total_invoice_value), 0) 
        COMMENT = 'Overall collection rate across customers' 
        SYNONYMS ('collection rate', 'cash collection rate', 'payment rate'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- PERFORMANCE METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    avg_customer_dso 
        AS AVG(avg_days_sales_outstanding) 
        COMMENT = 'Average DSO across all customers' 
        SYNONYMS ('average DSO', 'mean DSO', 'overall DSO', 'aggregate DSO'),
    
    median_customer_dso 
        AS MEDIAN(avg_days_sales_outstanding) 
        COMMENT = 'Median customer DSO' 
        SYNONYMS ('median DSO', '50th percentile DSO'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- SEGMENTATION METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    total_customers 
        AS COUNT(DISTINCT customer_name) 
        COMMENT = 'Total number of unique customers' 
        SYNONYMS ('customer count', 'number of customers', 'unique customers'),
    
    active_customers 
        AS COUNT_IF(total_order_count > 0) 
        COMMENT = 'Number of customers with at least one order' 
        SYNONYMS ('active customer count', 'customers with orders'),
    
    high_value_customers 
        AS COUNT_IF(total_order_value > 100000) 
        COMMENT = 'Number of customers with lifetime value over $100K' 
        SYNONYMS ('high value count', 'premium customers', 'top tier customers', '$100K+ customers'),
    
    at_risk_customers 
        AS COUNT_IF(avg_days_sales_outstanding > 60) 
        COMMENT = 'Number of customers with DSO over 60 days (at risk)' 
        SYNONYMS ('high DSO customers', 'slow pay customers', 'risk customers', '60+ day customers'),
    
    customers_with_ar 
        AS COUNT_IF(current_ar_outstanding > 0) 
        COMMENT = 'Number of customers with outstanding AR' 
        SYNONYMS ('AR customers', 'customers owing money', 'open balance customers'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- RISK METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    high_risk_customers 
        AS COUNT_IF(avg_days_sales_outstanding > 90) 
        COMMENT = 'Number of customers with DSO over 90 days (high risk)' 
        SYNONYMS ('very slow pay customers', 'high risk count', '90+ day customers'),
    
    high_value_at_risk 
        AS COUNT_IF(total_order_value > 100000 AND avg_days_sales_outstanding > 60) 
        COMMENT = 'Number of high-value customers that are at risk (>$100K LTV and >60 days DSO)' 
        SYNONYMS ('premium customers at risk', 'high value slow pay'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- GEOGRAPHIC METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    countries_served 
        AS COUNT(DISTINCT customer_country) 
        COMMENT = 'Number of unique countries with customers' 
        SYNONYMS ('country count', 'geographic coverage', 'markets served'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- DATA QUALITY METRICS (Enhanced Feature)
    -- ═══════════════════════════════════════════════════════════════
    
    customer_records 
        AS COUNT(*) 
        COMMENT = 'Total number of customer records' 
        SYNONYMS ('record count', 'row count', 'total records'),
    
    customers_updated_today 
        AS COUNT_IF(DATE(dbt_updated_at) = CURRENT_DATE()) 
        COMMENT = 'Number of customer records updated today' 
        SYNONYMS ('today updates', 'daily updates', 'customers refreshed today')
)

COMMENT = 'Enhanced customer summary semantic view with lifetime metrics and risk analysis for Cortex Analyst'

