{#
═══════════════════════════════════════════════════════════════════════════════
CUSTOM RECONCILIATION TEST: AUM Validation
═══════════════════════════════════════════════════════════════════════════════

Validates that:
  SUM(stg_holdings.quantity × NAV × FX) ≈ SUM(dm_aum_summary.total_market_value_eur)

Tolerance: 1% (accounts for rounding differences)

This test PASSES when it returns 0 rows (no mismatches).
It FAILS when any account × date has > 1% AUM discrepancy.

Tags: reconciliation, critical

═══════════════════════════════════════════════════════════════════════════════
#}

{{ config(
    tags=['reconciliation', 'critical', 'daily'],
    severity='error'
) }}

WITH source_aum AS (
    SELECT
        holding_date,
        account_id,
        SUM(market_value_eur) AS source_aum_eur
    FROM {{ ref('stg_holdings') }}
    GROUP BY holding_date, account_id
),

mart_aum AS (
    SELECT
        holding_date,
        account_id,
        SUM(total_market_value_eur) AS mart_aum_eur
    FROM {{ ref('dm_aum_summary') }}
    GROUP BY holding_date, account_id
),

comparison AS (
    SELECT
        COALESCE(s.holding_date, m.holding_date) AS holding_date,
        COALESCE(s.account_id, m.account_id) AS account_id,
        COALESCE(s.source_aum_eur, 0) AS source_aum_eur,
        COALESCE(m.mart_aum_eur, 0) AS mart_aum_eur,
        ABS(COALESCE(s.source_aum_eur, 0) - COALESCE(m.mart_aum_eur, 0)) AS aum_difference,
        CASE
            WHEN COALESCE(s.source_aum_eur, 0) = 0 THEN 100
            ELSE ABS(COALESCE(s.source_aum_eur, 0) - COALESCE(m.mart_aum_eur, 0))
                 / ABS(s.source_aum_eur) * 100
        END AS difference_pct
    FROM source_aum s
    FULL OUTER JOIN mart_aum m
        ON s.holding_date = m.holding_date
        AND s.account_id = m.account_id
)

SELECT *
FROM comparison
WHERE difference_pct > 1.0
