{{
    config(
        materialized='table',
        tags=['dimension', 'scd2', 'daily', 'critical'],
        query_tag='dbt_dim_client'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN: SCD-2 DIMENSION with SOFT DELETE
═══════════════════════════════════════════════════════════════════════════════

Built on dbt snapshot (snap_client) for change data capture.
Adds is_current, is_active, is_deleted, record_status flags.

Prerequisites: Run snapshot first
  dbt snapshot --select snap_client

Query Patterns:
  -- Current active clients only
  SELECT * FROM dim_client WHERE is_current AND is_active;
  
  -- Client as of specific date
  SELECT * FROM dim_client
  WHERE '2024-01-15' BETWEEN dbt_valid_from
    AND COALESCE(dbt_valid_to, '9999-12-31');

═══════════════════════════════════════════════════════════════════════════════
#}

WITH snapshot_data AS (
    SELECT * FROM {{ ref('snap_client') }}
),

latest_versions AS (
    SELECT
        client_id,
        MAX(dbt_valid_from) AS max_valid_from,
        MAX(CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END) AS has_current_version
    FROM snapshot_data
    GROUP BY client_id
),

enriched AS (
    SELECT
        -- Surrogate key (unique per version)
        snap.client_id || '|' || TO_VARCHAR(snap.dbt_valid_from, 'YYYYMMDDHH24MISS') AS client_key,
        snap.client_id AS client_business_key,

        -- Natural keys
        snap.client_id,

        -- Business attributes
        snap.client_name,
        snap.client_type,
        snap.client_segment,
        snap.domicile_country,
        snap.domicile_country_name,
        snap.risk_profile,
        snap.relationship_manager,
        snap.onboarding_date,
        snap.tax_id,
        snap.lei_code,

        -- SCD-2 validity
        snap.dbt_valid_from,
        snap.dbt_valid_to,
        snap.dbt_scd_id,
        snap.dbt_updated_at AS dbt_snapshot_updated_at,

        -- IS_CURRENT
        CASE
            WHEN snap.dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current,

        -- IS_DELETED (soft delete)
        CASE
            WHEN snap.dbt_valid_to IS NOT NULL
             AND lv.has_current_version = 0
             AND snap.dbt_valid_from = lv.max_valid_from
            THEN TRUE
            ELSE FALSE
        END AS is_deleted,

        -- DELETED_AT
        CASE
            WHEN snap.dbt_valid_to IS NOT NULL
             AND lv.has_current_version = 0
             AND snap.dbt_valid_from = lv.max_valid_from
            THEN snap.dbt_valid_to
            ELSE NULL
        END AS deleted_at,

        -- IS_ACTIVE
        CASE
            WHEN snap.dbt_valid_to IS NOT NULL
             AND lv.has_current_version = 0
            THEN FALSE
            WHEN snap.source_is_active = FALSE
            THEN FALSE
            WHEN snap.dbt_valid_to IS NULL AND snap.source_is_active = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_active,

        -- RECORD_STATUS
        CASE
            WHEN snap.dbt_valid_to IS NULL AND snap.source_is_active = TRUE
                THEN 'ACTIVE'
            WHEN snap.dbt_valid_to IS NULL AND snap.source_is_active = FALSE
                THEN 'INACTIVE'
            WHEN snap.dbt_valid_to IS NOT NULL
             AND lv.has_current_version = 0
             AND snap.dbt_valid_from = lv.max_valid_from
                THEN 'DELETED'
            ELSE 'HISTORICAL'
        END AS record_status,

        -- Change detection
        {{ row_hash([
            'snap.client_name',
            'snap.client_type',
            'snap.client_segment',
            'snap.domicile_country',
            'snap.risk_profile',
            'snap.relationship_manager'
        ]) }},

        -- Source timestamps
        snap.source_load_ts,
        snap.source_update_ts,

        -- Audit columns
        {{ audit_columns() }}

    FROM snapshot_data snap
    LEFT JOIN latest_versions lv
        ON snap.client_id = lv.client_id
)

SELECT * FROM enriched
