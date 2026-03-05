{#
═══════════════════════════════════════════════════════════════════════════════
ROW HASH MACRO
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate MD5 hash of specified columns for change detection

Usage:
  SELECT
      your_columns,
      {{ row_hash(['col1', 'col2', 'col3']) }} as dbt_row_hash
  FROM your_source

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
HASH KEY MACRO (For surrogate keys)
═══════════════════════════════════════════════════════════════════════════════

Purpose: Generate a hash-based surrogate key from multiple columns

Usage:
  SELECT
      {{ hash_key(['source_system', 'account_id', 'fund_id']) }} as holding_key,
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
