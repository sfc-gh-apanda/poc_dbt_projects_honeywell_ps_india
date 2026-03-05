{{
    config(
        materialized='view',
        tags=['staging', 'nav', 'daily'],
        query_tag='dbt_stg_nav_prices'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_NAV_PRICES - NAV Prices enriched with Fund metadata
═══════════════════════════════════════════════════════════════════════════════

Pattern: VIEW (always current)
Enrichment: Fund name, type, benchmark from DIM_FUND

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Price keys
    nav.price_date,
    nav.fund_id,
    MD5(nav.fund_id || '|' || TO_VARCHAR(nav.price_date, 'YYYYMMDD')) AS nav_key,

    -- Price data
    nav.nav_per_unit,
    nav.daily_return_pct,
    nav.ytd_return_pct,
    nav.currency AS nav_currency,

    -- Fund enrichment
    f.isin,
    f.fund_name,
    f.fund_type,
    f.asset_class,
    f.benchmark_id,
    f.ter_bps,

    -- Benchmark enrichment
    bm.benchmark_name,
    bm.benchmark_ticker,

    -- Source tracking
    nav.source_system,

    -- Audit columns
    {{ audit_columns() }}

FROM {{ source('dws_tran', 'FACT_NAV_PRICES') }} nav

LEFT JOIN {{ source('dws_master', 'DIM_FUND') }} f
    ON nav.fund_id = f.fund_id

LEFT JOIN {{ source('dws_ref', 'DIM_BENCHMARK') }} bm
    ON f.benchmark_id = bm.benchmark_id
