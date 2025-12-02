{{
    config(
        materialized='view',
        tags=['o2c', 'staging', 'orders']
    )
}}

/*
    O2C Enriched Orders Staging
    
    Joins sales orders with customer master data.
    This demonstrates JOIN IN STAGING LAYER.
*/

select
    -- Keys
    orders.source_system,
    orders.company_code,
    orders.order_id,
    orders.order_line,
    orders.source_system || '|' || orders.order_id || '|' || orders.order_line as order_key,
    
    -- Customer (from JOIN)
    orders.customer_id,
    cust.customer_name,
    cust.customer_type,
    cust.customer_country,
    cust.customer_country_name,
    
    -- Order details
    orders.order_number,
    orders.order_date,
    orders.order_quantity,
    orders.order_amount_lcl as order_amount,
    orders.currency_code as order_currency,
    orders.order_status,
    
    -- Organization
    orders.sales_org,
    orders.profit_center,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as _dbt_loaded_at

from {{ source('o2c_transactions', 'fact_sales_orders') }} orders

left join {{ source('o2c_master_data', 'dim_customer') }} cust
    on orders.customer_id = cust.customer_num_sk
    and orders.source_system = cust.source_system

where orders.order_date >= dateadd('year', -2, current_date())

