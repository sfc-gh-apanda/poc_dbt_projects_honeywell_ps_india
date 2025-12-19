{{ config(materialized='semantic_view') }}

tables (
    CUSTOMER as {{ source('o2c_enhanced_aggregates', 'agg_o2c_by_customer') }}
    primary key (customer_key)
)

facts (
    CUSTOMER.total_order_value,
    CUSTOMER.total_invoice_value,
    CUSTOMER.total_payment_value,
    CUSTOMER.current_ar_outstanding,
    CUSTOMER.total_order_count,
    CUSTOMER.total_invoice_count,
    CUSTOMER.total_payment_count,
    CUSTOMER.avg_days_sales_outstanding,
    CUSTOMER.avg_days_to_invoice,
    CUSTOMER.avg_days_to_payment,
    CUSTOMER.first_order_date,
    CUSTOMER.last_order_date
)

dimensions (
    CUSTOMER.customer_name,
    CUSTOMER.customer_type,
    CUSTOMER.customer_country,
    CUSTOMER.customer_classification,
    CUSTOMER.dbt_environment,
    CUSTOMER.dbt_source_model
)

metrics (
    CUSTOMER.customer_lifetime_revenue as SUM(total_order_value)
        WITH SYNONYMS = ('total revenue', 'total customer value', 'LTV'),
    CUSTOMER.avg_customer_lifetime_value as AVG(total_order_value)
        WITH SYNONYMS = ('average LTV', 'mean customer value'),
    CUSTOMER.median_customer_lifetime_value as MEDIAN(total_order_value)
        WITH SYNONYMS = ('median LTV'),
    CUSTOMER.customer_current_ar as SUM(current_ar_outstanding)
        WITH SYNONYMS = ('total AR', 'total receivables'),
    CUSTOMER.avg_customer_ar as AVG(current_ar_outstanding)
        WITH SYNONYMS = ('average AR', 'mean customer AR'),
    CUSTOMER.avg_customer_dso as AVG(avg_days_sales_outstanding)
        WITH SYNONYMS = ('average DSO', 'mean DSO'),
    CUSTOMER.median_customer_dso as MEDIAN(avg_days_sales_outstanding)
        WITH SYNONYMS = ('median DSO'),
    CUSTOMER.total_customers as COUNT(DISTINCT customer_name)
        WITH SYNONYMS = ('customer count', 'number of customers'),
    CUSTOMER.active_customers as COUNT_IF(total_order_count > 0)
        WITH SYNONYMS = ('active customer count'),
    CUSTOMER.high_value_customers as COUNT_IF(total_order_value > 100000)
        WITH SYNONYMS = ('high value count', 'premium customers'),
    CUSTOMER.at_risk_customers as COUNT_IF(avg_days_sales_outstanding > 60)
        WITH SYNONYMS = ('high DSO customers', 'slow pay customers'),
    CUSTOMER.customers_with_ar as COUNT_IF(current_ar_outstanding > 0)
        WITH SYNONYMS = ('AR customers', 'open balance customers'),
    CUSTOMER.high_risk_customers as COUNT_IF(avg_days_sales_outstanding > 90)
        WITH SYNONYMS = ('very slow pay customers', 'high risk count'),
    CUSTOMER.high_value_at_risk as COUNT_IF(total_order_value > 100000 AND avg_days_sales_outstanding > 60)
        WITH SYNONYMS = ('premium customers at risk'),
    CUSTOMER.countries_served as COUNT(DISTINCT customer_country)
        WITH SYNONYMS = ('country count', 'geographic coverage'),
    CUSTOMER.customer_records as COUNT(*)
        WITH SYNONYMS = ('record count', 'row count')
)
