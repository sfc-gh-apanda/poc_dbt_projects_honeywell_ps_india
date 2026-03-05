{#
═══════════════════════════════════════════════════════════════════════════════
SNAPSHOT: CLIENT (SCD-2 with Soft Delete)
═══════════════════════════════════════════════════════════════════════════════

Captures historical changes to client dimension using SCD-2.
When a record is deleted from source, it is soft-deleted (invalidated).

Execution:
  dbt snapshot --select snap_client
  OR in Snowflake Native dbt:
  EXECUTE DBT PROJECT dbt_dws_client_reporting ARGS = 'snapshot --select snap_client';

═══════════════════════════════════════════════════════════════════════════════
#}

{% snapshot snap_client %}

{{
    config(
        target_database='DWS_EDW',
        target_schema='DWS_SNAPSHOTS',
        unique_key='client_id',
        strategy='check',
        check_cols=[
            'client_name',
            'client_type',
            'client_segment',
            'domicile_country',
            'risk_profile',
            'relationship_manager',
            'is_active'
        ],
        invalidate_hard_deletes=True,
        tags=['scd2', 'daily', 'critical']
    )
}}

SELECT
    client_id,
    client_name,
    client_type,
    client_segment,
    domicile_country,
    domicile_country_name,
    risk_profile,
    relationship_manager,
    onboarding_date,
    tax_id,
    lei_code,
    is_active AS source_is_active,
    load_ts AS source_load_ts,
    update_ts AS source_update_ts,
    '{{ invocation_id }}' AS dbt_snapshot_run_id,
    CURRENT_TIMESTAMP() AS dbt_snapshot_at

FROM {{ source('dws_master', 'DIM_CLIENT') }}

WHERE client_id IS NOT NULL

{% endsnapshot %}
