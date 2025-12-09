{{
    config(
        materialized='view',
        snowflake_warehouse=get_warehouse(),
        tags=['staging', 'orders']
    )
}}

{# DEBUG: Log what config dbt resolved #}
{{ log(">>> MODEL CONFIG DEBUG: snowflake_warehouse = " ~ config.get('snowflake_warehouse', 'NOT SET'), info=True) }}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_ENRICHED_ORDERS - Orders with Customer Enrichment
═══════════════════════════════════════════════════════════════════════════════

Purpose: Join fact_sales_orders with dim_customer for customer details
Pattern: VIEW (always current, no materialization)
Audit: Full audit columns (uniform across all models)

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Order keys
    orders.source_system,
    orders.company_code,
    orders.order_id,
    orders.order_line,
    orders.source_system || '|' || orders.order_id || '|' || orders.order_line AS order_key,
    
    -- Order details
    orders.order_number,
    orders.order_date,
    orders.order_quantity,
    orders.order_amount_lcl AS order_amount,
    orders.currency_code AS order_currency,
    orders.order_status,
    
    -- Customer info (from enrichment)
    orders.customer_id,
    cust.customer_name,
    cust.customer_type,
    cust.customer_country,
    cust.customer_country_name,
    
    -- Organizational
    orders.sales_org,
    orders.profit_center,
    
    -- Audit columns (uniform set)
    {{ audit_columns() }}

FROM {{ source('corp_tran', 'FACT_SALES_ORDERS') }} orders

LEFT JOIN {{ source('corp_master', 'DIM_CUSTOMER') }} cust
    ON orders.customer_id = cust.customer_num_sk
    AND orders.source_system = cust.source_system

WHERE orders.order_date >= DATEADD('year', -2, CURRENT_DATE())
