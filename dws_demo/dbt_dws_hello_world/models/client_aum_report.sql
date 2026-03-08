{{ config(materialized='table') }}

SELECT
    holding_date,
    account_id,
    fund_id,
    quantity,
    market_value_local,
    currency
FROM {{ source('dws_tran', 'FACT_PORTFOLIO_HOLDINGS') }}
WHERE holding_date = (
    SELECT MAX(holding_date)
    FROM {{ source('dws_tran', 'FACT_PORTFOLIO_HOLDINGS') }}
)
