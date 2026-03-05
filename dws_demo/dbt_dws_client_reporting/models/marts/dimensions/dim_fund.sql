{{
    config(
        materialized='table',
        tags=['dimension', 'full_refresh', 'daily'],
        query_tag='dbt_dim_fund'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 1: FULL REFRESH (materialized='table')
═══════════════════════════════════════════════════════════════════════════════

Complete rebuild every run. Simple, idempotent.
Suitable for small reference/master data tables.

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Keys
    f.fund_id,
    f.isin,
    MD5(f.fund_id) AS fund_key,

    -- Business attributes
    f.fund_name,
    f.fund_type,
    f.asset_class,
    f.fund_currency,
    f.domicile,
    f.inception_date,
    f.ter_bps,
    ROUND(f.ter_bps / 100.0, 4) AS ter_pct,

    -- Benchmark
    f.benchmark_id,
    bm.benchmark_name,
    bm.benchmark_ticker,
    bm.provider AS benchmark_provider,

    -- Status
    f.is_active,

    -- Derived
    DATEDIFF('year', f.inception_date, CURRENT_DATE()) AS fund_age_years,

    -- Change detection
    {{ row_hash([
        'f.fund_name', 'f.fund_type', 'f.asset_class',
        'f.fund_currency', 'f.ter_bps', 'f.is_active'
    ]) }},

    -- Audit columns
    {{ audit_columns() }}

FROM {{ source('dws_master', 'DIM_FUND') }} f

LEFT JOIN {{ source('dws_ref', 'DIM_BENCHMARK') }} bm
    ON f.benchmark_id = bm.benchmark_id
