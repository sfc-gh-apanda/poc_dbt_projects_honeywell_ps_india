{{
    config(
        materialized='table',
        schema='o2c_core',
        tags=['o2c', 'mart', 'core', 'cycle_analysis'],
        access='public'
    )
}}

/*
    O2C Cycle Time Analysis Mart
    
    Focused view on O2C cycle times for performance analytics.
    Only includes completed transactions (orders with payments).
    
    Source: dm_o2c_reconciliation
    Grain: One row per completed O2C transaction
*/

select
    -- Keys
    source_system,
    order_key,
    invoice_key,
    payment_key,
    
    -- Dates
    order_date,
    invoice_date,
    payment_date,
    due_date,
    
    -- Customer
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Amounts
    order_amount,
    invoice_amount,
    payment_amount,
    
    -- Cycle time metrics
    days_order_to_invoice,
    days_invoice_to_payment,
    days_order_to_cash,
    days_past_due,
    
    -- Status
    payment_timing,
    
    -- Derived metrics
    case
        when days_order_to_cash <= 30 then 'EXCELLENT'
        when days_order_to_cash <= 60 then 'GOOD'
        when days_order_to_cash <= 90 then 'FAIR'
        else 'POOR'
    end as cycle_time_rating,
    
    case
        when days_past_due <= 0 then 'CURRENT'
        when days_past_due <= 30 then 'DELINQUENT_1_30'
        when days_past_due <= 60 then 'DELINQUENT_31_60'
        when days_past_due <= 90 then 'DELINQUENT_61_90'
        else 'DELINQUENT_90_PLUS'
    end as aging_bucket,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as loaded_at

from {{ ref('dm_o2c_reconciliation') }}

where reconciliation_status = 'CLOSED'  -- Only completed transactions
  and payment_date is not null
  and days_order_to_cash is not null

