{{
    config(
        materialized='incremental',
        pre_hook="{{ switch_warehouse() }}",
        incremental_strategy='append',
        on_schema_change='fail',
        tags=['events', 'append_only', 'pattern_example'],
        query_tag='dbt_fact_o2c_events'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 3: APPEND ONLY (incremental_strategy='append')
═══════════════════════════════════════════════════════════════════════════════

Description:
  - Insert new records only, never update existing
  - Uses INSERT INTO ... SELECT
  - Ideal for immutable event logs and audit trails

Testing This Pattern:
  1. First run: dbt run --select fact_o2c_events (creates table)
  2. Add new source events
  3. Second run: dbt run --select fact_o2c_events
  4. Verify:
     - New events are appended
     - Old events are unchanged
     - Each batch has unique dbt_batch_id
     - dbt_created_at = dbt_updated_at (append-only, no updates)

═══════════════════════════════════════════════════════════════════════════════
#}

WITH order_events AS (
    SELECT
        MD5(source_system || '|' || order_id || '|ORDER_CREATED') AS event_id,
        'ORDER_CREATED' AS event_type,
        source_system,
        order_id AS entity_id,
        'ORDER' AS entity_type,
        order_date AS event_timestamp,
        order_amount AS event_amount,
        customer_id,
        customer_name,
        order_status AS event_status,
        'Order created: ' || order_id AS event_description
    FROM {{ ref('stg_enriched_orders') }}
),

invoice_events AS (
    SELECT
        MD5(source_system || '|' || invoice_id || '|INVOICE_CREATED') AS event_id,
        'INVOICE_CREATED' AS event_type,
        source_system,
        invoice_id AS entity_id,
        'INVOICE' AS entity_type,
        invoice_date AS event_timestamp,
        invoice_amount AS event_amount,
        NULL AS customer_id,
        NULL AS customer_name,
        invoice_status AS event_status,
        'Invoice created: ' || invoice_id || ' for order: ' || order_id AS event_description
    FROM {{ ref('stg_enriched_invoices') }}
),

payment_events AS (
    SELECT
        MD5(source_system || '|' || payment_id || '|PAYMENT_RECEIVED') AS event_id,
        'PAYMENT_RECEIVED' AS event_type,
        source_system,
        payment_id AS entity_id,
        'PAYMENT' AS entity_type,
        payment_date AS event_timestamp,
        payment_amount AS event_amount,
        NULL AS customer_id,
        NULL AS customer_name,
        payment_status AS event_status,
        'Payment received: ' || payment_id || ' via ' || COALESCE(bank_name, 'Unknown') AS event_description
    FROM {{ ref('stg_enriched_payments') }}
),

all_events AS (
    SELECT * FROM order_events
    UNION ALL
    SELECT * FROM invoice_events
    UNION ALL
    SELECT * FROM payment_events
)

SELECT
    e.event_id,
    e.event_type,
    e.source_system,
    e.entity_id,
    e.entity_type,
    e.event_timestamp,
    e.event_amount,
    e.customer_id,
    e.customer_name,
    e.event_status,
    e.event_description,
    
    -- Audit columns (uniform set - append-only so created = updated)
    {{ audit_columns() }}

FROM all_events e

{% if is_incremental() %}
-- Only include events newer than the latest in the target
WHERE e.event_timestamp > (
    SELECT COALESCE(MAX(event_timestamp), '1900-01-01'::DATE)
    FROM {{ this }}
)
{% endif %}
