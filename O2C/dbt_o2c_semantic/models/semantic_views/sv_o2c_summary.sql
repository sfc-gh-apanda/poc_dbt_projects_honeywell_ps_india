{{
    config(
        materialized='dynamic_table',
        target_lag='1 hour',
        snowflake_warehouse='COMPUTE_WH',
        schema='o2c_semantic_views',
        tags=['o2c', 'semantic_view', 'summary']
    )
}}

/*
    O2C Summary Semantic View
    
    Business-friendly view with key O2C metrics aggregated by customer and period.
    Materialized as Snowflake DYNAMIC TABLE for auto-refresh.
    
    Source: dm_o2c_reconciliation
    Refresh: Every 1 hour
*/

select
    -- Time dimensions
    date_trunc('month', order_date) as order_month,
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month_num,
    
    -- Customer dimensions
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Order metrics
    count(distinct order_key) as total_orders,
    sum(order_amount) as total_order_value,
    
    -- Invoice metrics
    count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end) as total_invoices,
    sum(invoice_amount) as total_invoice_value,
    round(
        count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end) * 100.0 /
        nullif(count(distinct order_key), 0),
        2
    ) as billing_rate_pct,
    
    -- Payment metrics
    count(distinct case when payment_key != 'NOT_PAID' then payment_key end) as total_payments,
    sum(payment_amount) as total_cash_collected,
    round(
        count(distinct case when payment_key != 'NOT_PAID' then payment_key end) * 100.0 /
        nullif(count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end), 0),
        2
    ) as collection_rate_pct,
    
    -- Outstanding metrics
    sum(outstanding_amount) as total_ar_outstanding,
    sum(unbilled_amount) as total_unbilled,
    
    -- Performance metrics
    round(avg(days_order_to_invoice), 1) as avg_days_to_invoice,
    round(avg(days_invoice_to_payment), 1) as avg_days_to_payment,
    round(avg(days_order_to_cash), 1) as avg_days_sales_outstanding,
    percentile_cont(0.5) within group (order by days_order_to_cash) as median_dso,
    
    -- On-time payment metrics
    count(distinct case when payment_timing = 'ON_TIME' then payment_key end) as on_time_payments,
    round(
        count(distinct case when payment_timing = 'ON_TIME' then payment_key end) * 100.0 /
        nullif(count(distinct case when payment_key != 'NOT_PAID' then payment_key end), 0),
        2
    ) as on_time_payment_pct,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as view_refreshed_at

from {{ ref('dm_o2c_reconciliation') }}

where order_date >= dateadd('year', -2, current_date())

group by
    order_month,
    order_year,
    order_month_num,
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country

