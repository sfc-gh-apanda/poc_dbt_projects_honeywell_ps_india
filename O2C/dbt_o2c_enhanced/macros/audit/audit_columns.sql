{#
═══════════════════════════════════════════════════════════════════════════════
AUDIT COLUMNS MACRO
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate standardized audit columns for all models

Columns Generated:
  - dbt_run_id          : Unique identifier for the dbt run (invocation)
  - dbt_batch_id        : Unique identifier per model per run
  - dbt_loaded_at       : Timestamp when record was loaded
  - dbt_source_model    : Name of the dbt model that created the record
  - dbt_environment     : Target environment (dev/prod)

Usage:
  SELECT 
      your_columns,
      {{ audit_columns() }}
  FROM your_source

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro audit_columns() %}
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT COLUMNS - Automatically populated by dbt
    -- ═══════════════════════════════════════════════════════════════
    
    -- Run tracking: Unique ID for entire dbt execution
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    
    -- Batch tracking: Unique per model per run
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    
    -- Timestamp: When this record was loaded
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    
    -- Source tracking: Which model created this
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    
    -- Environment: dev or prod
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}


{#
═══════════════════════════════════════════════════════════════════════════════
AUDIT COLUMNS FOR INCREMENTAL MODELS (with create/update tracking)
═══════════════════════════════════════════════════════════════════════════════

Purpose: Audit columns that preserve dbt_created_at for existing records

Additional Columns:
  - dbt_created_at      : When record was first created (preserved on update)
  - dbt_updated_at      : When record was last modified (always current)

Usage in incremental model:
  SELECT 
      your_columns,
      {{ audit_columns_incremental() }}
  FROM your_source s
  {% if is_incremental() %}
  LEFT JOIN {{ this }} t ON s.key = t.key
  {% endif %}

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro audit_columns_incremental() %}
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT COLUMNS (Incremental with create/update tracking)
    -- ═══════════════════════════════════════════════════════════════
    
    -- Run tracking
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    
    -- Batch tracking
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    
    -- CREATE timestamp: Only set on first insert, preserved on update
    {% if is_incremental() %}
    COALESCE(t.dbt_created_at, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS dbt_created_at,
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


{#
═══════════════════════════════════════════════════════════════════════════════
MINIMAL AUDIT COLUMNS (Lightweight version)
═══════════════════════════════════════════════════════════════════════════════

Purpose: Just the essential audit columns for views or lightweight models

Usage:
  SELECT 
      your_columns,
      {{ audit_columns_minimal() }}
  FROM your_source

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro audit_columns_minimal() %}
    -- Minimal audit columns
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at
{% endmacro %}


