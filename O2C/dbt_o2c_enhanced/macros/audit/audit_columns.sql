{#
═══════════════════════════════════════════════════════════════════════════════
AUDIT COLUMNS MACRO - STANDARDIZED FOR ALL MODELS
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate standardized audit columns for all models (views, tables, incremental)

Columns Generated (UNIFORM across all models):
  - dbt_run_id          : Unique identifier for the dbt run (invocation)
  - dbt_batch_id        : Unique identifier per model per run
  - dbt_loaded_at       : Timestamp when record was loaded
  - dbt_created_at      : When record was created (= dbt_updated_at for non-incremental)
  - dbt_updated_at      : When record was last modified (always current)
  - dbt_source_model    : Name of the dbt model that created the record
  - dbt_environment     : Target environment (dev/prod)

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


{# ═══════════════════════════════════════════════════════════════════════════
   STANDARD AUDIT COLUMNS - For views, tables, full-refresh models
   For non-incremental: dbt_created_at = dbt_updated_at = current timestamp
   ═══════════════════════════════════════════════════════════════════════════ #}

{% macro audit_columns() %}
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT COLUMNS - Uniform set for all models
    -- ═══════════════════════════════════════════════════════════════
    
    -- Run tracking: Unique ID for entire dbt execution
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    
    -- Batch tracking: Unique per model per run
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    
    -- Timestamp: When this record was loaded
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    
    -- CREATE timestamp: For non-incremental, same as updated (all rows recreated)
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    
    -- UPDATE timestamp: When record was last modified
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    
    -- Source tracking: Which model created this
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    
    -- Environment: dev or prod
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}


{# ═══════════════════════════════════════════════════════════════════════════
   INCREMENTAL AUDIT COLUMNS - For incremental models with merge/upsert
   Preserves dbt_created_at for existing records, updates dbt_updated_at
   ═══════════════════════════════════════════════════════════════════════════ #}

{% macro audit_columns_incremental(existing_alias='existing') %}
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT COLUMNS (Incremental - preserves create timestamp)
    -- ═══════════════════════════════════════════════════════════════
    
    -- Run tracking
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    
    -- Batch tracking
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    
    -- Loaded timestamp
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    
    -- CREATE timestamp: Preserved for existing rows, set for new rows
    {% if is_incremental() %}
    COALESCE({{ existing_alias }}.dbt_created_at, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS dbt_created_at,
    {% else %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    {% endif %}
    
    -- UPDATE timestamp: Always current
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    
    -- Source tracking
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    
    -- Environment
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}
