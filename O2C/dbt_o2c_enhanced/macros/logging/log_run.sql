{#
═══════════════════════════════════════════════════════════════════════════════
RUN LOGGING MACROS
═══════════════════════════════════════════════════════════════════════════════

Purpose: Log dbt run start and end to tracking tables

Prerequisites:
  - Run O2C_AUDIT_SETUP.sql to create tracking tables
  - Tables: EDW.O2C_AUDIT.DBT_RUN_LOG

═══════════════════════════════════════════════════════════════════════════════
#}


{#
═══════════════════════════════════════════════════════════════════════════════
LOG RUN START
═══════════════════════════════════════════════════════════════════════════════

Called by: on-run-start hook in dbt_project.yml

Purpose: Insert a new record into DBT_RUN_LOG when a dbt run begins

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro log_run_start() %}
    {% if var('enable_audit_logging', true) %}
        {% set sql %}
            INSERT INTO EDW.O2C_AUDIT.DBT_RUN_LOG (
                run_id,
                project_name,
                project_version,
                environment,
                run_started_at,
                run_status,
                run_command,
                warehouse_name,
                user_name,
                role_name,
                selector_used
            )
            SELECT
                '{{ invocation_id }}',
                '{{ project_name }}',
                '{{ var("dbt_version", "unknown") }}',
                '{{ target.name }}',
                '{{ run_started_at }}'::TIMESTAMP_NTZ,
                'RUNNING',
                '{{ flags.WHICH if flags is defined and flags.WHICH is defined else "unknown" }}',
                CURRENT_WAREHOUSE(),
                CURRENT_USER(),
                CURRENT_ROLE(),
                '{{ invocation_args_dict.get("select", ["all"]) | join(",") if invocation_args_dict is defined else "all" }}'
            WHERE NOT EXISTS (
                SELECT 1 FROM EDW.O2C_AUDIT.DBT_RUN_LOG 
                WHERE run_id = '{{ invocation_id }}'
            );
        {% endset %}
        
        {% do run_query(sql) %}
        {% do log("✅ Run logging started: " ~ invocation_id, info=true) %}
    {% endif %}
{% endmacro %}


{#
═══════════════════════════════════════════════════════════════════════════════
LOG RUN END
═══════════════════════════════════════════════════════════════════════════════

Called by: on-run-end hook in dbt_project.yml

Purpose: Update DBT_RUN_LOG with final status and counts when dbt run completes

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro log_run_end() %}
    {% if var('enable_audit_logging', true) %}
        {% set sql %}
            UPDATE EDW.O2C_AUDIT.DBT_RUN_LOG
            SET 
                run_ended_at = CURRENT_TIMESTAMP(),
                run_duration_seconds = DATEDIFF('second', run_started_at, CURRENT_TIMESTAMP()),
                run_status = CASE 
                    WHEN (SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                          WHERE run_id = '{{ invocation_id }}' AND status = 'FAIL') > 0 
                    THEN 'FAILED'
                    WHEN (SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                          WHERE run_id = '{{ invocation_id }}' AND status = 'ERROR') > 0 
                    THEN 'ERROR'
                    ELSE 'SUCCESS'
                END,
                models_run = (SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                              WHERE run_id = '{{ invocation_id }}'),
                models_success = (SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                                  WHERE run_id = '{{ invocation_id }}' AND status = 'SUCCESS'),
                models_failed = (SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                                 WHERE run_id = '{{ invocation_id }}' AND status IN ('FAIL', 'ERROR')),
                models_skipped = (SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
                                  WHERE run_id = '{{ invocation_id }}' AND status = 'SKIPPED')
            WHERE run_id = '{{ invocation_id }}';
        {% endset %}
        
        {% do run_query(sql) %}
        {% do log("✅ Run logging completed: " ~ invocation_id, info=true) %}
    {% endif %}
{% endmacro %}


