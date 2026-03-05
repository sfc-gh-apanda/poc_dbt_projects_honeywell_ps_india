{{
    config(
        materialized='view',
        tags=['staging', 'holdings', 'daily'],
        query_tag='dbt_stg_holdings'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_HOLDINGS - Portfolio Holdings enriched with Fund & FX data
═══════════════════════════════════════════════════════════════════════════════

Pattern: VIEW (always current, no materialization)
Enrichment: Joins fund metadata and FX rates at staging layer
            so downstream marts don't repeat this logic.

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Position keys
    h.holding_date,
    h.account_id,
    h.fund_id,
    MD5(h.account_id || '|' || h.fund_id || '|' || TO_VARCHAR(h.holding_date, 'YYYYMMDD')) AS holding_key,

    -- Position details
    h.quantity,
    h.cost_price,
    h.cost_value_local,
    h.market_value_local,
    h.currency AS holding_currency,

    -- Fund enrichment
    f.isin,
    f.fund_name,
    f.fund_type,
    f.asset_class,
    f.fund_currency,
    f.benchmark_id,

    -- NAV enrichment
    nav.nav_per_unit,
    nav.daily_return_pct,
    nav.ytd_return_pct,

    -- Calculated: market value using NAV (qty × NAV)
    ROUND(h.quantity * COALESCE(nav.nav_per_unit, 0), 2) AS calculated_market_value_local,

    -- FX to EUR
    COALESCE(fx.exchange_rate, 1.0) AS fx_rate_to_eur,
    ROUND(h.quantity * COALESCE(nav.nav_per_unit, 0) * COALESCE(fx.exchange_rate, 1.0), 2) AS market_value_eur,

    -- Source tracking
    h.source_system,

    -- Audit columns
    {{ audit_columns() }}

FROM {{ source('dws_tran', 'FACT_PORTFOLIO_HOLDINGS') }} h

LEFT JOIN {{ source('dws_master', 'DIM_FUND') }} f
    ON h.fund_id = f.fund_id

LEFT JOIN {{ source('dws_tran', 'FACT_NAV_PRICES') }} nav
    ON h.fund_id = nav.fund_id
    AND h.holding_date = nav.price_date

LEFT JOIN {{ source('dws_ref', 'FACT_FX_RATES') }} fx
    ON h.currency = fx.from_currency
    AND h.holding_date = fx.rate_date
    AND fx.to_currency = 'EUR'
