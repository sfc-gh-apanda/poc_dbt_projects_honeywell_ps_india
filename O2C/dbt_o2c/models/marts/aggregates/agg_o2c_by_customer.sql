{{
    config(
        materialized='table',
        schema='o2c_aggregates',
        tags=['o2c', 'mart', 'aggregate', 'customer'],
        access='public'
    )
}}

/*
    O2C Aggregated Metrics by Customer
    
    Pre-aggregated summary for fast customer-level reporting
*/

select
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Counts
    count(distinct order_key) as total_orders,
    count(distinct case when invoice_key != 'NOT_INVOICED' then invoice_key end) as total_invoices,
    count(distinct case when payment_key != 'NOT_PAID' then payment_key end) as total_payments,
    
    -- Amounts
    sum(order_amount) as total_order_amount,
    sum(invoice_amount) as total_invoice_amount,
    sum(payment_amount) as total_payment_amount,
    sum(outstanding_amount) as total_outstanding,
    
    -- Performance
    avg(days_order_to_cash) as avg_days_to_cash,
    avg(days_past_due) as avg_days_past_due,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as loaded_at
    
from {{ ref('dm_o2c_reconciliation') }}

group by 1, 2, 3, 4, 5

