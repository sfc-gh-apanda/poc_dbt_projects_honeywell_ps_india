{#
═══════════════════════════════════════════════════════════════════════════════
ROW HASH MACRO
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate MD5 hash of specified columns for change detection

Use Cases:
  - Detect changes in incremental models
  - SCD Type 2 change detection
  - Data reconciliation between systems

Usage:
  SELECT 
      your_columns,
      {{ row_hash(['col1', 'col2', 'col3']) }} as dbt_row_hash
  FROM your_source

Example:
  {{ row_hash(['customer_name', 'customer_type', 'customer_country']) }}
  -- Generates: MD5(CONCAT_WS('|', ...))

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro row_hash(columns, alias='dbt_row_hash') %}
    MD5(
        CONCAT_WS('||',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '__NULL__')
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
    )::VARCHAR(32) AS {{ alias }}
{% endmacro %}


{#
═══════════════════════════════════════════════════════════════════════════════
ROW HASH FROM ALL COLUMNS (Automatic)
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate hash from all non-audit columns in a CTE/subquery

Usage (in incremental model):
  WITH source_data AS (
      SELECT * FROM {{ source('schema', 'table') }}
  )
  SELECT 
      *,
      {{ row_hash_auto(['id', 'dbt_'], exclude_prefixes=true) }}
  FROM source_data

Note: Requires explicit column list for Snowflake

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro row_hash_exclude(columns, exclude_columns, alias='dbt_row_hash') %}
    {# Filter out excluded columns #}
    {% set filtered_columns = [] %}
    {% for col in columns %}
        {% if col not in exclude_columns %}
            {% do filtered_columns.append(col) %}
        {% endif %}
    {% endfor %}
    
    {{ row_hash(filtered_columns, alias) }}
{% endmacro %}


{#
═══════════════════════════════════════════════════════════════════════════════
HASH KEY MACRO (For surrogate keys)
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate a hash-based surrogate key from multiple columns

Usage:
  SELECT 
      {{ hash_key(['source_system', 'order_id', 'line_number']) }} as order_key,
      other_columns
  FROM your_source

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro hash_key(columns, alias='hash_key') %}
    MD5(
        CONCAT_WS('|',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '')
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
    )::VARCHAR(32) AS {{ alias }}
{% endmacro %}


