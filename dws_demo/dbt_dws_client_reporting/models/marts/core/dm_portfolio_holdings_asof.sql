{{
    config(
        materialized='table',
        tags=['mart', 'core', 'daily', 'as_of_reporting', 'critical', 'reconciliation'],
        query_tag='dbt_dm_portfolio_holdings_asof'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
AS-OF DATE REPORTING PATTERN
═══════════════════════════════════════════════════════════════════════════════

Shows portfolio holdings as of a specific date.
Supports point-in-time queries via var('as_of_date').

Usage:
  -- Default: current date
  dbt run --select dm_portfolio_holdings_asof

  -- Specific date:
  dbt run --select dm_portfolio_holdings_asof --vars '{"as_of_date": "''2024-01-31''"}'

═══════════════════════════════════════════════════════════════════════════════
#}

WITH as_of_holdings AS (
    SELECT
        h.*
    FROM {{ ref('stg_holdings') }} h
    WHERE h.holding_date = (
        SELECT MAX(holding_date)
        FROM {{ ref('stg_holdings') }}
        WHERE holding_date <= {{ var('as_of_date', 'CURRENT_DATE()') }}
    )
)

SELECT
    -- As-of date
    h.holding_date AS as_of_date,

    -- Account context
    h.account_id,
    a.client_id,
    a.client_name,
    a.client_type,
    a.client_segment,
    a.account_name,
    a.account_type,
    a.mandate_type,
    a.base_currency AS account_currency,

    -- Fund context
    h.fund_id,
    h.isin,
    h.fund_name,
    h.fund_type,
    h.asset_class,

    -- Position details
    h.quantity,
    h.nav_per_unit,
    h.holding_currency,

    -- Valuations (local currency)
    h.cost_value_local,
    h.market_value_local,
    h.calculated_market_value_local,

    -- Valuations (EUR)
    h.fx_rate_to_eur,
    h.market_value_eur,
    ROUND(h.cost_value_local * h.fx_rate_to_eur, 2) AS cost_value_eur,

    -- P&L
    ROUND(h.market_value_eur - (h.cost_value_local * h.fx_rate_to_eur), 2) AS unrealized_pnl_eur,
    CASE
        WHEN h.cost_value_local > 0
        THEN ROUND((h.market_value_eur - (h.cost_value_local * h.fx_rate_to_eur))
             / (h.cost_value_local * h.fx_rate_to_eur) * 100, 4)
        ELSE 0
    END AS unrealized_pnl_pct,

    -- Performance
    h.daily_return_pct,
    h.ytd_return_pct,

    -- Audit columns
    {{ audit_columns() }}

FROM as_of_holdings h

LEFT JOIN {{ ref('dim_account') }} a
    ON h.account_id = a.account_id
