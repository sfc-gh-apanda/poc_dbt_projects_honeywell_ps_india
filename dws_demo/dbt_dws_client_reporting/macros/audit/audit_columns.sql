{#
═══════════════════════════════════════════════════════════════════════════════
AUDIT COLUMNS MACRO - STANDARDIZED FOR ALL MODELS
═══════════════════════════════════════════════════════════════════════════════

Columns Generated (UNIFORM across all models):
  - dbt_run_id          : Unique identifier for the dbt run (invocation)
  - dbt_batch_id        : Unique identifier per model per run
  - dbt_loaded_at       : Timestamp when record was loaded
  - dbt_created_at      : When record was created (= dbt_updated_at for non-incremental)
  - dbt_updated_at      : When record was last modified (always current)
  - dbt_source_model    : Name of the dbt model that created the record
  - dbt_environment     : Target environment (dev/test/prod)

Usage (for views, tables, full-refresh models):
  SELECT
      your_columns,
      {{ audit_columns() }}
  FROM your_source

Usage (for incremental models - preserves dbt_created_at):
  SELECT
      your_columns,
      {{ audit_columns_incremental('existing_alias') }}
  FROM your_source s
  {% if is_incremental() %}
  LEFT JOIN {{ this }} existing_alias ON s.key = existing_alias.key
  {% endif %}

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro audit_columns() %}
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}


{% macro audit_columns_incremental(existing_alias='existing') %}
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    {% if is_incremental() %}
    COALESCE({{ existing_alias }}.dbt_created_at, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS dbt_created_at,
    {% else %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    {% endif %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}
