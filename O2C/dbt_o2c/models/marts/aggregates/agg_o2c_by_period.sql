{{
    config(
        materialized='table',
        schema='o2c_aggregates',
        tags=['o2c', 'mart', 'aggregate', 'period'],
        access='public'
    )
}}

/*
    O2C Aggregated Metrics by Period
    
    Time-series aggregation for trend analysis.
    
    Source: dm_o2c_reconciliation
    Grain: One row per month per source system
*/

select
    source_system,
    
    -- Time dimensions
    date_trunc('month', order_date) as order_month,
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month_num,
    
    -- Volume metrics
    count(distinct order_key) as total_orders,
    count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end) as total_invoices,
    count(distinct case when payment_key != 'NOT_PAID' then payment_key end) as total_payments,
    
    -- Amount metrics
    sum(order_amount) as total_order_amount,
    sum(invoice_amount) as total_invoice_amount,
    sum(payment_amount) as total_payment_amount,
    sum(outstanding_amount) as total_outstanding_amount,
    
    -- Performance metrics
    avg(days_order_to_invoice) as avg_days_to_invoice,
    avg(days_invoice_to_payment) as avg_days_to_payment,
    avg(days_order_to_cash) as avg_dso,
    percentile_cont(0.5) within group (order by days_order_to_cash) as median_dso,
    percentile_cont(0.9) within group (order by days_order_to_cash) as p90_dso,
    
    -- Ratios
    round(
        count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end) * 100.0 /
        nullif(count(distinct order_key), 0),
        2
    ) as billing_rate_pct,
    
    round(
        count(distinct case when payment_key != 'NOT_PAID' then payment_key end) * 100.0 /
        nullif(count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end), 0),
        2
    ) as collection_rate_pct,
    
    round(
        count(distinct case when payment_timing = 'ON_TIME' then payment_key end) * 100.0 /
        nullif(count(distinct case when payment_key != 'NOT_PAID' then payment_key end), 0),
        2
    ) as on_time_payment_pct,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as loaded_at

from {{ ref('dm_o2c_reconciliation') }}

where order_date is not null

group by 1, 2, 3, 4

