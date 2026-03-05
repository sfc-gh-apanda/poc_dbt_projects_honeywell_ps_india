{{
    config(
        materialized='view',
        tags=['staging', 'transactions', 'daily'],
        query_tag='dbt_stg_transactions'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_TRANSACTIONS - Transactions enriched with Fund & Account data
═══════════════════════════════════════════════════════════════════════════════

Pattern: VIEW (always current)
Enrichment: Fund metadata, account details, FX rates

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Transaction keys
    t.transaction_id,
    MD5(t.transaction_id || '|' || t.account_id) AS transaction_key,
    t.transaction_date,
    t.settlement_date,

    -- Account & Fund
    t.account_id,
    t.fund_id,
    a.client_id,
    a.account_name,
    a.account_type,
    a.mandate_type,
    a.base_currency AS account_currency,

    -- Fund enrichment
    f.isin,
    f.fund_name,
    f.fund_type,
    f.asset_class,

    -- Transaction details
    t.transaction_type,
    t.quantity,
    t.price_per_unit,
    t.gross_amount,
    t.fees,
    t.tax_amount,
    t.net_amount,
    t.currency AS transaction_currency,

    -- Derived flags
    CASE
        WHEN t.transaction_type IN ('BUY', 'TRANSFER_IN') THEN 'INFLOW'
        WHEN t.transaction_type IN ('SELL', 'TRANSFER_OUT') THEN 'OUTFLOW'
        WHEN t.transaction_type = 'DIVIDEND' THEN 'INCOME'
        WHEN t.transaction_type = 'FEE' THEN 'COST'
        ELSE 'OTHER'
    END AS flow_direction,

    -- Net flow amount (inflows positive, outflows negative)
    CASE
        WHEN t.transaction_type IN ('BUY', 'TRANSFER_IN') THEN ABS(t.net_amount)
        WHEN t.transaction_type IN ('SELL', 'TRANSFER_OUT') THEN -ABS(t.net_amount)
        ELSE 0
    END AS net_flow_amount,

    -- FX to EUR
    COALESCE(fx.exchange_rate, 1.0) AS fx_rate_to_eur,
    ROUND(t.net_amount * COALESCE(fx.exchange_rate, 1.0), 2) AS net_amount_eur,

    -- Source tracking
    t.source_system,

    -- Audit columns
    {{ audit_columns() }}

FROM {{ source('dws_tran', 'FACT_TRANSACTIONS') }} t

LEFT JOIN {{ source('dws_master', 'DIM_ACCOUNT') }} a
    ON t.account_id = a.account_id

LEFT JOIN {{ source('dws_master', 'DIM_FUND') }} f
    ON t.fund_id = f.fund_id

LEFT JOIN {{ source('dws_ref', 'FACT_FX_RATES') }} fx
    ON t.currency = fx.from_currency
    AND t.transaction_date = fx.rate_date
    AND fx.to_currency = 'EUR'
