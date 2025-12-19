{{ config(materialized='semantic_view') }}

tables (
    CUSTOMER as {{ source('o2c_enhanced_aggregates', 'agg_o2c_by_customer') }}
    primary key (customer_id)
)

facts (
    CUSTOMER.total_order_value as total_order_value,
    CUSTOMER.total_invoice_value as total_invoice_value,
    CUSTOMER.total_payment_value as total_payment_value,
    CUSTOMER.total_outstanding as total_outstanding,
    CUSTOMER.total_orders as total_orders,
    CUSTOMER.total_invoices as total_invoices,
    CUSTOMER.total_payments as total_payments,
    CUSTOMER.avg_dso as avg_dso,
    CUSTOMER.median_dso as median_dso,
    CUSTOMER.max_dso as max_dso,
    CUSTOMER.on_time_payments as on_time_payments,
    CUSTOMER.late_payments as late_payments,
    CUSTOMER.overdue_count as overdue_count,
    CUSTOMER.billing_rate_pct as billing_rate_pct,
    CUSTOMER.collection_rate_pct as collection_rate_pct,
    CUSTOMER.on_time_payment_pct as on_time_payment_pct,
    CUSTOMER.first_order_date as first_order_date,
    CUSTOMER.last_order_date as last_order_date
)

dimensions (
    CUSTOMER.customer_id as customer_id,
    CUSTOMER.customer_name as customer_name,
    CUSTOMER.customer_type as customer_type,
    CUSTOMER.customer_country as customer_country,
    CUSTOMER.source_system as source_system,
    CUSTOMER.payment_behavior as payment_behavior,
    CUSTOMER.dbt_environment as dbt_environment,
    CUSTOMER.dbt_source_model as dbt_source_model
)

metrics (
    CUSTOMER.customer_lifetime_revenue as SUM(total_order_value)
        WITH SYNONYMS = ('total revenue', 'total customer value', 'LTV', 'lifetime value'),
    CUSTOMER.avg_customer_lifetime_value as AVG(total_order_value)
        WITH SYNONYMS = ('average LTV', 'mean customer value'),
    CUSTOMER.median_customer_lifetime_value as MEDIAN(total_order_value)
        WITH SYNONYMS = ('median LTV'),
    CUSTOMER.customer_current_ar as SUM(total_outstanding)
        WITH SYNONYMS = ('total AR', 'total receivables', 'outstanding AR'),
    CUSTOMER.avg_customer_ar as AVG(total_outstanding)
        WITH SYNONYMS = ('average AR', 'mean customer AR'),
    CUSTOMER.avg_customer_dso as AVG(avg_dso)
        WITH SYNONYMS = ('average DSO', 'mean DSO', 'days sales outstanding'),
    CUSTOMER.median_customer_dso as MEDIAN(median_dso)
        WITH SYNONYMS = ('median DSO'),
    CUSTOMER.total_customers as COUNT(DISTINCT customer_name)
        WITH SYNONYMS = ('customer count', 'number of customers'),
    CUSTOMER.total_order_count as SUM(total_orders)
        WITH SYNONYMS = ('total orders', 'order count'),
    CUSTOMER.total_invoice_count as SUM(total_invoices)
        WITH SYNONYMS = ('total invoices', 'invoice count'),
    CUSTOMER.total_payment_count as SUM(total_payments)
        WITH SYNONYMS = ('total payments', 'payment count'),
    CUSTOMER.high_value_customers as COUNT_IF(total_order_value > 100000)
        WITH SYNONYMS = ('high value count', 'premium customers'),
    CUSTOMER.at_risk_customers as COUNT_IF(avg_dso > 60)
        WITH SYNONYMS = ('high DSO customers', 'slow pay customers'),
    CUSTOMER.customers_with_ar as COUNT_IF(total_outstanding > 0)
        WITH SYNONYMS = ('AR customers', 'open balance customers'),
    CUSTOMER.high_risk_customers as COUNT_IF(avg_dso > 90)
        WITH SYNONYMS = ('very slow pay customers', 'high risk count'),
    CUSTOMER.high_value_at_risk as COUNT_IF(total_order_value > 100000 AND avg_dso > 60)
        WITH SYNONYMS = ('premium customers at risk'),
    CUSTOMER.excellent_payment_customers as COUNT_IF(payment_behavior = 'EXCELLENT')
        WITH SYNONYMS = ('excellent payers', 'top customers'),
    CUSTOMER.poor_payment_customers as COUNT_IF(payment_behavior = 'POOR')
        WITH SYNONYMS = ('poor payers', 'problem customers'),
    CUSTOMER.countries_served as COUNT(DISTINCT customer_country)
        WITH SYNONYMS = ('country count', 'geographic coverage'),
    CUSTOMER.customer_records as COUNT(*)
        WITH SYNONYMS = ('record count', 'row count')
)
