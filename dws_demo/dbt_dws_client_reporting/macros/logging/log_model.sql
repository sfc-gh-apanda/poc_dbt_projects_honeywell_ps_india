{#
═══════════════════════════════════════════════════════════════════════════════
MODEL LOGGING MACRO
═══════════════════════════════════════════════════════════════════════════════

Called by: post-hook in dbt_project.yml
Table:     DWSEDW.DWS_AUDIT.DBT_MODEL_LOG

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro log_model_execution() %}
    {% if var('enable_audit_logging', true) and execute %}
        {% set log_id = invocation_id ~ '_' ~ this.name %}
        {% set batch_id = modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') ~ '_' ~ this.name %}
        {% set audit_db = var('audit_database', 'DWSEDW') %}
        {% set audit_sch = var('audit_schema', 'DWS_AUDIT') %}

        {% set sql %}
            INSERT INTO {{ audit_db }}.{{ audit_sch }}.DBT_MODEL_LOG (
                log_id, run_id, project_name, model_name, model_alias,
                schema_name, database_name, materialization, batch_id,
                status, started_at, ended_at, is_incremental, incremental_strategy
            )
            SELECT
                '{{ log_id }}'::VARCHAR(200),
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
                SELECT 1 FROM {{ audit_db }}.{{ audit_sch }}.DBT_MODEL_LOG
                WHERE log_id = '{{ log_id }}'
            );
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}


{% macro log_model_with_row_count() %}
    {% if var('enable_audit_logging', true) and execute %}
        {% set log_id = invocation_id ~ '_' ~ this.name %}
        {% set batch_id = modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') ~ '_' ~ this.name %}
        {% set audit_db = var('audit_database', 'DWSEDW') %}
        {% set audit_sch = var('audit_schema', 'DWS_AUDIT') %}

        {% set sql %}
            INSERT INTO {{ audit_db }}.{{ audit_sch }}.DBT_MODEL_LOG (
                log_id, run_id, project_name, model_name,
                schema_name, database_name, materialization, batch_id,
                status, started_at, ended_at, rows_affected, is_incremental
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
                SELECT 1 FROM {{ audit_db }}.{{ audit_sch }}.DBT_MODEL_LOG
                WHERE log_id = '{{ log_id }}'
            );
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
