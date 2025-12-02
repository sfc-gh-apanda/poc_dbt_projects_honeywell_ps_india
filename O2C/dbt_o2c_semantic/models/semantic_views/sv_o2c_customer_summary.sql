{{
    config(
        materialized='semantic_view'
    )
}}

/*
    O2C Customer Summary Semantic View
    
    Enables Cortex Analyst to answer natural language questions about:
    - Customer lifetime value
    - Customer payment behavior
    - Customer DSO and performance
    
    Materialization: Snowflake SEMANTIC VIEW (using dbt_semantic_view package)
    Source: Pre-aggregated customer metrics
*/

TABLES(
    {{ ref('agg_o2c_by_customer') }}
)
DIMENSIONS(
    customer_name COMMENT = 'Customer company name' SYNONYMS ('customer', 'company', 'account'),
    customer_type COMMENT = 'Customer type: E=External, I=Internal' SYNONYMS ('account type', 'customer category'),
    customer_country COMMENT = 'Customer country' SYNONYMS ('country', 'location', 'geography'),
    source_system COMMENT = 'Source ERP system' SYNONYMS ('system', 'ERP')
)
FACTS(
    total_orders COMMENT = 'Total lifetime orders for customer' SYNONYMS ('order count', 'number of orders'),
    total_invoices COMMENT = 'Total lifetime invoices' SYNONYMS ('invoice count'),
    total_payments COMMENT = 'Total lifetime payments' SYNONYMS ('payment count'),
    total_order_amount COMMENT = 'Total lifetime order value' SYNONYMS ('customer revenue', 'customer sales', 'lifetime value', 'LTV'),
    total_invoice_amount COMMENT = 'Total lifetime invoice value' SYNONYMS ('billed amount'),
    total_payment_amount COMMENT = 'Total lifetime cash collected' SYNONYMS ('cash collected', 'collections'),
    total_outstanding COMMENT = 'Current AR outstanding for customer' SYNONYMS ('customer AR', 'customer receivables', 'unpaid'),
    avg_days_to_cash COMMENT = 'Average DSO for customer' SYNONYMS ('customer DSO', 'customer collection time'),
    avg_days_past_due COMMENT = 'Average days past due for customer'
)
METRICS(
    customer_lifetime_revenue AS SUM(total_order_amount) COMMENT = 'Sum of customer lifetime revenue' SYNONYMS ('total customer revenue'),
    customer_current_ar AS SUM(total_outstanding) COMMENT = 'Sum of current AR outstanding' SYNONYMS ('total customer AR'),
    avg_customer_dso AS AVG(avg_days_to_cash) COMMENT = 'Average DSO across customers',
    total_customers AS COUNT(DISTINCT customer_id) COMMENT = 'Total number of customers' SYNONYMS ('customer count')
)
COMMENT = 'O2C customer-level metrics semantic view for Cortex Analyst'

