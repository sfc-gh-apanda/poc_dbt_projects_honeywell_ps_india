{{ config(materialized='semantic_view') }}

tables (
    RECONCILIATION as {{ source('o2c_enhanced_core', 'dm_o2c_reconciliation') }}
    primary key (order_key, invoice_key, payment_key)
)

facts (
    RECONCILIATION.order_amount,
    RECONCILIATION.invoice_amount,
    RECONCILIATION.payment_amount,
    RECONCILIATION.outstanding_amount,
    RECONCILIATION.unbilled_amount,
    RECONCILIATION.days_order_to_invoice,
    RECONCILIATION.days_invoice_to_payment,
    RECONCILIATION.days_order_to_cash,
    RECONCILIATION.days_past_due,
    RECONCILIATION.order_quantity
)

dimensions (
    RECONCILIATION.customer_name,
    RECONCILIATION.customer_type,
    RECONCILIATION.customer_country,
    RECONCILIATION.source_system,
    RECONCILIATION.reconciliation_status,
    RECONCILIATION.payment_timing,
    RECONCILIATION.order_date,
    RECONCILIATION.invoice_date,
    RECONCILIATION.payment_date,
    RECONCILIATION.due_date,
    RECONCILIATION.dbt_environment,
    RECONCILIATION.dbt_source_model
)

metrics (
    RECONCILIATION.total_revenue as SUM(order_amount)
        WITH SYNONYMS = ('total sales', 'total order value', 'revenue'),
    RECONCILIATION.avg_order_value as AVG(order_amount)
        WITH SYNONYMS = ('average order', 'mean order value'),
    RECONCILIATION.total_orders as COUNT(DISTINCT order_key)
        WITH SYNONYMS = ('order count', 'number of orders'),
    RECONCILIATION.total_ar_outstanding as SUM(outstanding_amount)
        WITH SYNONYMS = ('total receivables', 'total AR', 'AR outstanding'),
    RECONCILIATION.total_unbilled as SUM(unbilled_amount)
        WITH SYNONYMS = ('total backlog', 'pending billing'),
    RECONCILIATION.total_invoices as COUNT(DISTINCT invoice_key)
        WITH SYNONYMS = ('invoice count', 'number of invoices'),
    RECONCILIATION.total_payments as COUNT(DISTINCT payment_key)
        WITH SYNONYMS = ('payment count', 'number of payments'),
    RECONCILIATION.avg_dso as AVG(days_order_to_cash)
        WITH SYNONYMS = ('average days sales outstanding', 'mean DSO', 'DSO'),
    RECONCILIATION.median_dso as MEDIAN(days_order_to_cash)
        WITH SYNONYMS = ('median collection period'),
    RECONCILIATION.avg_days_to_invoice as AVG(days_order_to_invoice)
        WITH SYNONYMS = ('average billing time'),
    RECONCILIATION.avg_days_to_payment as AVG(days_invoice_to_payment)
        WITH SYNONYMS = ('average collection time'),
    RECONCILIATION.record_count as COUNT(*)
        WITH SYNONYMS = ('row count', 'total records'),
    RECONCILIATION.distinct_customers as COUNT(DISTINCT customer_name)
        WITH SYNONYMS = ('customer count', 'unique customers'),
    RECONCILIATION.distinct_countries as COUNT(DISTINCT customer_country)
        WITH SYNONYMS = ('country count')
)
