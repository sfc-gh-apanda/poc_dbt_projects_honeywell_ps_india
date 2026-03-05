{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        tags=['events', 'append_only', 'daily'],
        query_tag='dbt_fact_client_events'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 3: APPEND ONLY (incremental_strategy='append')
═══════════════════════════════════════════════════════════════════════════════

Immutable audit trail of all client transaction events.
New events appended, old events never touched.

═══════════════════════════════════════════════════════════════════════════════
#}

WITH transaction_events AS (
    SELECT
        MD5(t.transaction_id || '|' || t.transaction_type) AS event_id,
        t.transaction_type AS event_type,
        t.transaction_date AS event_date,
        t.account_id,
        t.client_id,
        t.fund_id,
        t.transaction_id AS entity_id,
        'TRANSACTION' AS entity_type,
        t.net_amount_eur AS event_amount_eur,
        t.transaction_currency AS event_currency,
        t.fund_name,
        t.account_name,
        t.flow_direction,
        t.transaction_type || ': ' || t.fund_name || ' (' || t.account_name || ')' AS event_description,
        t.source_system
    FROM {{ ref('stg_transactions') }} t
)

SELECT
    e.event_id,
    e.event_type,
    e.event_date,
    e.account_id,
    e.client_id,
    e.fund_id,
    e.entity_id,
    e.entity_type,
    e.event_amount_eur,
    e.event_currency,
    e.fund_name,
    e.account_name,
    e.flow_direction,
    e.event_description,
    e.source_system,

    -- Audit columns
    {{ audit_columns() }}

FROM transaction_events e

{% if is_incremental() %}
WHERE e.event_date > (
    SELECT COALESCE(MAX(event_date), '1900-01-01'::DATE)
    FROM {{ this }}
)
{% endif %}
