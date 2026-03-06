{{
    config(
        materialized='table',
        tags=['mart', 'core', 'daily', 'full_refresh', 'critical'],
        query_tag='dbt_dm_client_performance'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
CLIENT PERFORMANCE MART
═══════════════════════════════════════════════════════════════════════════════

Calculates portfolio-level performance metrics per client and account.
Uses time-weighted return approximation from NAV daily returns.

Pattern: FULL REFRESH (table) — recalculated daily

═══════════════════════════════════════════════════════════════════════════════
#}

WITH daily_account_values AS (
    SELECT
        h.holding_date,
        h.account_id,
        SUM(h.market_value_eur) AS total_aum_eur,
        SUM(h.cost_value_local * h.fx_rate_to_eur) AS total_cost_eur,
        AVG(h.daily_return_pct) AS weighted_daily_return
    FROM {{ ref('stg_holdings') }} h
    GROUP BY h.holding_date, h.account_id
),

account_flows AS (
    SELECT
        t.account_id,
        t.transaction_date,
        SUM(t.net_flow_amount * t.fx_rate_to_eur) AS net_flow_eur
    FROM {{ ref('stg_transactions') }} t
    WHERE t.flow_direction IN ('INFLOW', 'OUTFLOW')
    GROUP BY t.account_id, t.transaction_date
),

account_income AS (
    SELECT
        t.account_id,
        SUM(CASE WHEN t.transaction_type = 'DIVIDEND' THEN t.net_amount_eur ELSE 0 END) AS total_dividends_eur,
        SUM(CASE WHEN t.transaction_type = 'FEE' THEN ABS(t.net_amount_eur) ELSE 0 END) AS total_fees_eur,
        COUNT(DISTINCT CASE WHEN t.transaction_type IN ('BUY','SELL') THEN t.transaction_id END) AS total_trades
    FROM {{ ref('stg_transactions') }} t
    GROUP BY t.account_id
),

account_summary AS (
    SELECT
        dav.account_id,
        MIN(dav.holding_date) AS period_start,
        MAX(dav.holding_date) AS period_end,
        DATEDIFF('day', MIN(dav.holding_date), MAX(dav.holding_date)) AS period_days,

        -- Opening & closing AUM
        FIRST_VALUE(dav.total_aum_eur) OVER (
            PARTITION BY dav.account_id ORDER BY dav.holding_date
        ) AS opening_aum_eur,
        LAST_VALUE(dav.total_aum_eur) OVER (
            PARTITION BY dav.account_id ORDER BY dav.holding_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS closing_aum_eur,

        -- Average AUM
        AVG(dav.total_aum_eur) AS avg_aum_eur,

        -- Total cost
        LAST_VALUE(dav.total_cost_eur) OVER (
            PARTITION BY dav.account_id ORDER BY dav.holding_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS total_cost_eur,

        -- Approximate TWR (product of daily returns)
        AVG(dav.weighted_daily_return) AS avg_daily_return_pct

    FROM daily_account_values dav
    GROUP BY dav.account_id, dav.total_aum_eur, dav.total_cost_eur,
             dav.holding_date, dav.weighted_daily_return
)

SELECT
    -- Account context
    s.account_id,
    a.client_id,
    a.client_name,
    a.client_type,
    a.account_name,
    a.mandate_type,
    a.base_currency,

    -- Period
    MIN(s.period_start) AS period_start,
    MAX(s.period_end) AS period_end,
    MAX(s.period_days) AS period_days,

    -- AUM
    MIN(s.opening_aum_eur) AS opening_aum_eur,
    MAX(s.closing_aum_eur) AS closing_aum_eur,
    AVG(s.avg_aum_eur) AS avg_aum_eur,

    -- P&L
    MAX(s.closing_aum_eur) - MAX(s.total_cost_eur) AS total_unrealized_pnl_eur,

    -- Performance (simplified)
    CASE
        WHEN MIN(s.opening_aum_eur) > 0
        THEN ROUND((MAX(s.closing_aum_eur) - MIN(s.opening_aum_eur))
             / MIN(s.opening_aum_eur) * 100, 4)
        ELSE 0
    END AS total_return_pct,

    -- Net flows
    COALESCE(SUM(af.net_flow_eur), 0) AS total_net_flows_eur,

    -- Income & costs
    COALESCE(MAX(ai.total_dividends_eur), 0) AS total_dividends_eur,
    COALESCE(MAX(ai.total_fees_eur), 0) AS total_fees_eur,
    COALESCE(MAX(ai.total_trades), 0) AS total_trades,

    -- Audit columns
    {{ audit_columns() }}

FROM account_summary s

LEFT JOIN {{ ref('dim_account') }} a
    ON s.account_id = a.account_id

LEFT JOIN account_flows af
    ON s.account_id = af.account_id

LEFT JOIN account_income ai
    ON s.account_id = ai.account_id

GROUP BY
    s.account_id, a.client_id, a.client_name, a.client_type,
    a.account_name, a.mandate_type, a.base_currency,
    ai.total_dividends_eur, ai.total_fees_eur, ai.total_trades
