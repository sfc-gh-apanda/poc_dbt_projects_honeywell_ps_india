{{
    config(
        materialized='table',
        tags=['aggregate', 'daily', 'critical'],
        query_tag='dbt_agg_client_overview'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
CLIENT OVERVIEW AGGREGATE
═══════════════════════════════════════════════════════════════════════════════

Executive-level client KPI summary. One row per client.
Dashboard-optimized for fast query performance.

═══════════════════════════════════════════════════════════════════════════════
#}

WITH latest_aum AS (
    SELECT
        client_id,
        client_name,
        client_type,
        SUM(total_market_value_eur) AS current_aum_eur,
        SUM(total_cost_value_eur) AS current_cost_eur,
        SUM(total_unrealized_pnl_eur) AS current_pnl_eur,
        COUNT(DISTINCT account_id) AS num_active_accounts,
        COUNT(DISTINCT fund_id) AS num_fund_positions,
        COUNT(DISTINCT mandate_type) AS num_mandate_types,
        COUNT(DISTINCT asset_class) AS num_asset_classes
    FROM {{ ref('dm_aum_summary') }}
    WHERE holding_date = (SELECT MAX(holding_date) FROM {{ ref('dm_aum_summary') }})
    GROUP BY client_id, client_name, client_type
),

client_flows AS (
    SELECT
        client_id,
        SUM(gross_inflows_eur) AS total_inflows_eur,
        SUM(gross_outflows_eur) AS total_outflows_eur,
        SUM(net_flows_eur) AS total_net_flows_eur,
        SUM(dividends_eur) AS total_dividends_eur,
        SUM(fees_eur) AS total_fees_eur,
        SUM(total_transactions) AS total_transactions
    FROM {{ ref('dm_cashflow_summary') }}
    GROUP BY client_id
),

perf AS (
    SELECT
        client_id,
        SUM(opening_aum_eur) AS total_opening_aum_eur,
        SUM(closing_aum_eur) AS total_closing_aum_eur,
        SUM(total_trades) AS total_trades
    FROM {{ ref('dm_client_performance') }}
    GROUP BY client_id
)

SELECT
    -- Client identity
    a.client_id,
    a.client_name,
    a.client_type,

    -- AUM
    a.current_aum_eur,
    a.current_cost_eur,
    a.current_pnl_eur,
    CASE
        WHEN a.current_cost_eur > 0
        THEN ROUND(a.current_pnl_eur / a.current_cost_eur * 100, 4)
        ELSE 0
    END AS unrealized_return_pct,

    -- Portfolio composition
    a.num_active_accounts,
    a.num_fund_positions,
    a.num_mandate_types,
    a.num_asset_classes,

    -- Flows
    COALESCE(cf.total_inflows_eur, 0) AS total_inflows_eur,
    COALESCE(cf.total_outflows_eur, 0) AS total_outflows_eur,
    COALESCE(cf.total_net_flows_eur, 0) AS total_net_flows_eur,
    COALESCE(cf.total_dividends_eur, 0) AS total_dividends_eur,
    COALESCE(cf.total_fees_eur, 0) AS total_fees_eur,
    COALESCE(cf.total_transactions, 0) AS total_transactions,

    -- Performance
    COALESCE(p.total_trades, 0) AS total_trades,

    -- Client tier (by AUM)
    CASE
        WHEN a.current_aum_eur >= 100000000 THEN 'TIER_1'
        WHEN a.current_aum_eur >= 10000000 THEN 'TIER_2'
        WHEN a.current_aum_eur >= 1000000 THEN 'TIER_3'
        ELSE 'TIER_4'
    END AS client_tier,

    -- Audit columns
    {{ audit_columns() }}

FROM latest_aum a

LEFT JOIN client_flows cf
    ON a.client_id = cf.client_id

LEFT JOIN perf p
    ON a.client_id = p.client_id
