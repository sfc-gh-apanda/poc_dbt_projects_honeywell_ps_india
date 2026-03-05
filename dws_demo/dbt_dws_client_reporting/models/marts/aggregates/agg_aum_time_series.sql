{{
    config(
        materialized='table',
        tags=['aggregate', 'daily', 'weekly'],
        query_tag='dbt_agg_aum_time_series'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
AUM TIME SERIES AGGREGATE
═══════════════════════════════════════════════════════════════════════════════

Daily and monthly AUM rollups for time-series dashboards.
Pre-calculated for fast query performance.

═══════════════════════════════════════════════════════════════════════════════
#}

WITH daily_aum AS (
    SELECT
        holding_date,
        client_id,
        client_name,
        client_type,
        mandate_type,
        asset_class,

        SUM(total_market_value_eur) AS daily_aum_eur,
        SUM(total_cost_value_eur) AS daily_cost_eur,
        SUM(total_unrealized_pnl_eur) AS daily_pnl_eur,
        SUM(num_positions) AS daily_positions

    FROM {{ ref('dm_aum_summary') }}
    GROUP BY holding_date, client_id, client_name, client_type, mandate_type, asset_class
)

SELECT
    -- Time dimensions
    d.holding_date,
    DATE_TRUNC('month', d.holding_date) AS month_start,
    TO_VARCHAR(d.holding_date, 'YYYY-MM') AS year_month,
    DAYOFWEEK(d.holding_date) AS day_of_week,
    CASE WHEN d.holding_date = LAST_DAY(d.holding_date) THEN TRUE ELSE FALSE END AS is_month_end,

    -- Client dimensions
    d.client_id,
    d.client_name,
    d.client_type,
    d.mandate_type,
    d.asset_class,

    -- Daily AUM
    d.daily_aum_eur,
    d.daily_cost_eur,
    d.daily_pnl_eur,
    d.daily_positions,

    -- Day-over-day change
    d.daily_aum_eur - LAG(d.daily_aum_eur) OVER (
        PARTITION BY d.client_id, d.mandate_type, d.asset_class
        ORDER BY d.holding_date
    ) AS aum_change_eur,

    CASE
        WHEN LAG(d.daily_aum_eur) OVER (
            PARTITION BY d.client_id, d.mandate_type, d.asset_class
            ORDER BY d.holding_date) > 0
        THEN ROUND(
            (d.daily_aum_eur - LAG(d.daily_aum_eur) OVER (
                PARTITION BY d.client_id, d.mandate_type, d.asset_class
                ORDER BY d.holding_date))
            / LAG(d.daily_aum_eur) OVER (
                PARTITION BY d.client_id, d.mandate_type, d.asset_class
                ORDER BY d.holding_date) * 100, 4)
        ELSE 0
    END AS aum_change_pct,

    -- Audit columns
    {{ audit_columns() }}

FROM daily_aum d
