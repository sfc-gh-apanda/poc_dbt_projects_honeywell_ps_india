{{
    config(
        materialized='dynamic_table',
        target_lag='1 hour',
        snowflake_warehouse='COMPUTE_WH',
        schema='o2c_semantic_views',
        tags=['o2c', 'semantic_view', 'customer']
    )
}}

/*
    O2C Customer Metrics Semantic View
    
    Customer-centric view for customer analytics and collections.
    Materialized as Snowflake DYNAMIC TABLE.
    
    Source: dm_o2c_reconciliation
    Refresh: Every 1 hour
*/

select
    -- Customer dimensions
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Volume metrics
    count(distinct order_key) as lifetime_orders,
    count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end) as lifetime_invoices,
    count(distinct case when payment_key != 'NOT_PAID' then payment_key end) as lifetime_payments,
    
    -- Value metrics (lifetime)
    sum(order_amount) as lifetime_order_value,
    sum(invoice_amount) as lifetime_invoice_value,
    sum(payment_amount) as lifetime_cash_collected,
    
    -- Current outstanding
    sum(case when reconciliation_status in ('NOT_PAID', 'OPEN') then outstanding_amount else 0 end) as current_ar_outstanding,
    sum(case when reconciliation_status = 'NOT_INVOICED' then unbilled_amount else 0 end) as current_unbilled,
    
    -- Overdue analysis
    sum(case when payment_timing = 'OVERDUE' then outstanding_amount else 0 end) as overdue_amount,
    count(distinct case when payment_timing = 'OVERDUE' then invoice_key end) as overdue_invoices,
    max(case when payment_timing = 'OVERDUE' then days_past_due end) as max_days_overdue,
    
    -- Performance metrics
    round(avg(days_order_to_cash), 1) as avg_dso,
    percentile_cont(0.5) within group (order by days_order_to_cash) as median_dso,
    round(avg(days_past_due), 1) as avg_days_past_due,
    
    -- Payment behavior
    round(
        count(distinct case when payment_timing = 'ON_TIME' then payment_key end) * 100.0 /
        nullif(count(distinct case when payment_key != 'NOT_PAID' then payment_key end), 0),
        2
    ) as on_time_payment_rate,
    
    -- Ratios
    round(
        sum(payment_amount) * 100.0 / nullif(sum(invoice_amount), 0),
        2
    ) as collection_rate_pct,
    
    -- Last activity dates
    max(order_date) as last_order_date,
    max(case when invoice_id is not null then invoice_date end) as last_invoice_date,
    max(case when payment_id is not null then payment_date end) as last_payment_date,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as view_refreshed_at

from {{ ref('dm_o2c_reconciliation') }}

group by
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country

