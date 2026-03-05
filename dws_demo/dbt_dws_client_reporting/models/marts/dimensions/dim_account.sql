{{
    config(
        materialized='table',
        tags=['dimension', 'full_refresh', 'daily'],
        query_tag='dbt_dim_account'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 1: FULL REFRESH (materialized='table')
═══════════════════════════════════════════════════════════════════════════════

Complete rebuild every run for account/mandate dimension.

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Keys
    a.account_id,
    a.client_id,
    MD5(a.account_id || '|' || a.client_id) AS account_key,

    -- Business attributes
    a.account_name,
    a.account_type,
    a.mandate_type,
    a.base_currency,
    a.inception_date,
    a.management_fee_bps,
    ROUND(a.management_fee_bps / 100.0, 4) AS management_fee_pct,

    -- Benchmark
    a.benchmark_id,
    bm.benchmark_name,

    -- Client enrichment
    c.client_name,
    c.client_type,
    c.client_segment,
    c.domicile_country,

    -- Status
    a.is_active,

    -- Derived
    DATEDIFF('year', a.inception_date, CURRENT_DATE()) AS account_age_years,

    -- Change detection
    {{ row_hash([
        'a.account_name', 'a.account_type', 'a.mandate_type',
        'a.base_currency', 'a.management_fee_bps', 'a.is_active'
    ]) }},

    -- Audit columns
    {{ audit_columns() }}

FROM {{ source('dws_master', 'DIM_ACCOUNT') }} a

LEFT JOIN {{ source('dws_master', 'DIM_CLIENT') }} c
    ON a.client_id = c.client_id

LEFT JOIN {{ source('dws_ref', 'DIM_BENCHMARK') }} bm
    ON a.benchmark_id = bm.benchmark_id
