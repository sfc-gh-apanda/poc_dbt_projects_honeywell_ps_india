{#
═══════════════════════════════════════════════════════════════════════════════
MODEL LOGGING MACRO
═══════════════════════════════════════════════════════════════════════════════

Purpose: Log individual model executions to tracking table

Called by: post-hook in dbt_project.yml

Prerequisites:
  - Run O2C_AUDIT_SETUP.sql to create tracking tables
  - Table: EDW.O2C_AUDIT.DBT_MODEL_LOG

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro log_model_execution() %}
    {% if var('enable_audit_logging', true) and execute %}
        
        {# Generate unique log ID and batch ID #}
        {% set log_id = invocation_id ~ '_' ~ this.name %}
        {% set batch_id = modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') ~ '_' ~ this.name %}
        
        {% set sql %}
            INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (
                log_id,
                run_id,
                project_name,
                model_name,
                model_alias,
                schema_name,
                database_name,
                materialization,
                batch_id,
                status,
                started_at,
                ended_at,
                is_incremental,
                incremental_strategy
            )
            SELECT
                '{{ log_id }}'::VARCHAR(100),
                '{{ invocation_id }}',
                '{{ project_name }}',
                '{{ this.name }}',
                '{{ this.alias if this.alias else this.name }}',
                '{{ this.schema }}',
                '{{ this.database }}',
                '{{ config.get("materialized", "view") }}',
                '{{ batch_id }}',
                'SUCCESS',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                {{ 'TRUE' if config.get("materialized") == 'incremental' else 'FALSE' }},
                '{{ config.get("incremental_strategy", "default") }}'
            WHERE NOT EXISTS (
                SELECT 1 FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                WHERE log_id = '{{ log_id }}'
            );
        {% endset %}
        
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}


{#
═══════════════════════════════════════════════════════════════════════════════
LOG MODEL WITH ROW COUNT
═══════════════════════════════════════════════════════════════════════════════

Purpose: Enhanced model logging with row count capture

Usage: Add to post-hook for specific models that need row count tracking

Example in model config:
  {{ config(
      post_hook="{{ log_model_with_row_count() }}"
  ) }}

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro log_model_with_row_count() %}
    {% if var('enable_audit_logging', true) and execute %}
        
        {% set log_id = invocation_id ~ '_' ~ this.name %}
        {% set batch_id = modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') ~ '_' ~ this.name %}
        
        {% set sql %}
            -- First, insert the log entry
            INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (
                log_id,
                run_id,
                project_name,
                model_name,
                schema_name,
                database_name,
                materialization,
                batch_id,
                status,
                started_at,
                ended_at,
                rows_affected,
                is_incremental
            )
            SELECT
                '{{ log_id }}',
                '{{ invocation_id }}',
                '{{ project_name }}',
                '{{ this.name }}',
                '{{ this.schema }}',
                '{{ this.database }}',
                '{{ config.get("materialized", "view") }}',
                '{{ batch_id }}',
                'SUCCESS',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                (SELECT COUNT(*) FROM {{ this }}),
                {{ 'TRUE' if config.get("materialized") == 'incremental' else 'FALSE' }}
            WHERE NOT EXISTS (
                SELECT 1 FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                WHERE log_id = '{{ log_id }}'
            );
        {% endset %}
        
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}


{#
═══════════════════════════════════════════════════════════════════════════════
LOG MODEL FAILURE
═══════════════════════════════════════════════════════════════════════════════

Purpose: Log model execution failures with error details

Usage: This is typically handled by dbt's built-in error handling
       Can be called manually in custom error handling scenarios

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro log_model_failure(error_message) %}
    {% if var('enable_audit_logging', true) %}
        
        {% set log_id = invocation_id ~ '_' ~ this.name ~ '_FAIL' %}
        
        {% set sql %}
            INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (
                log_id,
                run_id,
                project_name,
                model_name,
                schema_name,
                database_name,
                materialization,
                batch_id,
                status,
                error_message,
                started_at,
                ended_at
            )
            VALUES (
                '{{ log_id }}',
                '{{ invocation_id }}',
                '{{ project_name }}',
                '{{ this.name }}',
                '{{ this.schema }}',
                '{{ this.database }}',
                '{{ config.get("materialized", "view") }}',
                NULL,
                'FAIL',
                '{{ error_message | replace("'", "''") }}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
        {% endset %}
        
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}


