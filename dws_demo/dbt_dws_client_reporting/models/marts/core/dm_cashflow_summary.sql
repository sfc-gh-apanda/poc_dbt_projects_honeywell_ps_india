{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'transaction_date', 'fund_id'],
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.transaction_date >= DATEADD('day', -" ~ var('reload_days', 3) ~ ", CURRENT_DATE())"
        ],
        tags=['mart', 'core', 'daily', 'delete_insert'],
        query_tag='dbt_dm_cashflow_summary'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 4: DELETE + INSERT (incremental_strategy='delete+insert')
═══════════════════════════════════════════════════════════════════════════════

Daily cashflow summary by account × fund × date.
Reloads last N days to catch late-arriving transactions.

Usage:
  dbt run --select dm_cashflow_summary
  dbt run --select dm_cashflow_summary --vars '{"reload_days": 7}'

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Keys
    t.transaction_date,
    t.account_id,
    t.fund_id,
    t.client_id,

    -- Context
    t.client_id AS client_key,
    t.account_name,
    t.mandate_type,
    t.fund_name,
    t.fund_type,

    -- Inflows
    SUM(CASE WHEN t.flow_direction = 'INFLOW' THEN ABS(t.net_amount_eur) ELSE 0 END) AS gross_inflows_eur,
    COUNT(CASE WHEN t.flow_direction = 'INFLOW' THEN 1 END) AS inflow_count,

    -- Outflows
    SUM(CASE WHEN t.flow_direction = 'OUTFLOW' THEN ABS(t.net_amount_eur) ELSE 0 END) AS gross_outflows_eur,
    COUNT(CASE WHEN t.flow_direction = 'OUTFLOW' THEN 1 END) AS outflow_count,

    -- Net flows
    SUM(CASE
        WHEN t.flow_direction = 'INFLOW' THEN ABS(t.net_amount_eur)
        WHEN t.flow_direction = 'OUTFLOW' THEN -ABS(t.net_amount_eur)
        ELSE 0
    END) AS net_flows_eur,

    -- Income
    SUM(CASE WHEN t.transaction_type = 'DIVIDEND' THEN t.net_amount_eur ELSE 0 END) AS dividends_eur,
    SUM(CASE WHEN t.transaction_type = 'FEE' THEN ABS(t.net_amount_eur) ELSE 0 END) AS fees_eur,

    -- Total transactions
    COUNT(DISTINCT t.transaction_id) AS total_transactions,

    -- Audit columns
    {{ audit_columns() }}

FROM {{ ref('stg_transactions') }} t

WHERE t.transaction_date >= DATEADD('day', -{{ var('reload_days', 3) }}, CURRENT_DATE())

GROUP BY
    t.transaction_date, t.account_id, t.fund_id, t.client_id,
    t.account_name, t.mandate_type, t.fund_name, t.fund_type
