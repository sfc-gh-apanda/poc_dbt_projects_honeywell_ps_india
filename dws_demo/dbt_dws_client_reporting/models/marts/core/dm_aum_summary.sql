{{
    config(
        materialized='incremental',
        unique_key='aum_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        merge_update_columns=[
            'total_market_value_local', 'total_market_value_eur',
            'total_cost_value_eur', 'total_unrealized_pnl_eur',
            'num_positions', 'avg_nav_per_unit',
            'dbt_updated_at', 'dbt_run_id', 'dbt_batch_id',
            'dbt_loaded_at', 'dbt_source_model', 'dbt_environment', 'dbt_row_hash'
        ],
        tags=['mart', 'core', 'daily', 'merge', 'critical', 'reconciliation'],
        query_tag='dbt_dm_aum_summary'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 2: INCREMENTAL MERGE / UPSERT
═══════════════════════════════════════════════════════════════════════════════

AUM (Assets Under Management) summary by client × account × fund × date.
New dates INSERT, existing dates UPDATE.
dbt_created_at preserved for existing records.

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Composite key
    MD5(h.account_id || '|' || h.fund_id || '|' || TO_VARCHAR(h.holding_date, 'YYYYMMDD')) AS aum_key,

    -- Dimensions
    h.holding_date,
    h.account_id,
    a.client_id,
    a.client_name,
    a.client_type,
    a.account_name,
    a.mandate_type,
    h.fund_id,
    h.fund_name,
    h.fund_type,
    h.asset_class,
    h.holding_currency,

    -- AUM metrics
    SUM(h.market_value_local) AS total_market_value_local,
    SUM(h.market_value_eur) AS total_market_value_eur,
    SUM(h.cost_value_local * h.fx_rate_to_eur) AS total_cost_value_eur,
    SUM(h.market_value_eur) - SUM(h.cost_value_local * h.fx_rate_to_eur) AS total_unrealized_pnl_eur,
    COUNT(*) AS num_positions,
    AVG(h.nav_per_unit) AS avg_nav_per_unit,

    -- Change detection
    {{ row_hash([
        'SUM(h.market_value_eur)',
        'SUM(h.cost_value_local * h.fx_rate_to_eur)',
        'COUNT(*)'
    ]) }},

    -- Audit columns (incremental - preserves dbt_created_at)
    {{ audit_columns_incremental('existing') }}

FROM {{ ref('stg_holdings') }} h

LEFT JOIN {{ ref('dim_account') }} a
    ON h.account_id = a.account_id

{% if is_incremental() %}
LEFT JOIN {{ this }} existing
    ON MD5(h.account_id || '|' || h.fund_id || '|' || TO_VARCHAR(h.holding_date, 'YYYYMMDD')) = existing.aum_key
{% endif %}

GROUP BY
    h.holding_date, h.account_id, a.client_id, a.client_name,
    a.client_type, a.account_name, a.mandate_type,
    h.fund_id, h.fund_name, h.fund_type, h.asset_class, h.holding_currency
    {% if is_incremental() %}
    , existing.dbt_created_at
    {% endif %}
