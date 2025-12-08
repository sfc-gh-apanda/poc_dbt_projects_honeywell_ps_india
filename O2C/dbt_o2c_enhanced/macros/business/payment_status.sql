{% macro payment_status(payment_date, due_date) %}
{#-
================================================================================
    CLASSIFY PAYMENT TIMING STATUS
================================================================================

    Purpose:
        Standardize payment timing classification across all models.

    Parameters:
        payment_date: Column name for actual payment date
        due_date: Column name for payment due date

    Returns:
        - 'ON_TIME'  : Paid on or before due date
        - 'LATE'     : Paid after due date
        - 'OVERDUE'  : Not paid and past due date
        - 'CURRENT'  : Not paid but not yet past due date

    Usage:
        {{ payment_status('payment_date', 'due_date') }}
        
    Example:
        SELECT
            order_id,
            {{ payment_status('payment_date', 'due_date') }} AS payment_timing

================================================================================
-#}

    CASE
        WHEN {{ payment_date }} IS NOT NULL AND {{ payment_date }} <= {{ due_date }} THEN 'ON_TIME'
        WHEN {{ payment_date }} IS NOT NULL AND {{ payment_date }} > {{ due_date }} THEN 'LATE'
        WHEN {{ payment_date }} IS NULL AND CURRENT_DATE() > {{ due_date }} THEN 'OVERDUE'
        ELSE 'CURRENT'
    END

{% endmacro %}


{% macro payment_timing_bucket(payment_date, due_date) %}
{#-
    More detailed payment timing buckets for aging analysis
    
    Returns:
        - 'EARLY'        : Paid 7+ days before due
        - 'ON_TIME'      : Paid within 7 days of due date
        - 'LATE_1_30'    : Paid 1-30 days late
        - 'LATE_31_60'   : Paid 31-60 days late
        - 'LATE_61_90'   : Paid 61-90 days late
        - 'LATE_90_PLUS' : Paid 90+ days late
        - 'UNPAID_CURRENT'  : Not paid, not yet due
        - 'UNPAID_1_30'     : Not paid, 1-30 days overdue
        - 'UNPAID_31_60'    : Not paid, 31-60 days overdue
        - 'UNPAID_61_90'    : Not paid, 61-90 days overdue
        - 'UNPAID_90_PLUS'  : Not paid, 90+ days overdue
-#}

    CASE
        -- Paid scenarios
        WHEN {{ payment_date }} IS NOT NULL THEN
            CASE
                WHEN DATEDIFF('day', {{ payment_date }}, {{ due_date }}) >= 7 THEN 'EARLY'
                WHEN {{ payment_date }} <= {{ due_date }} THEN 'ON_TIME'
                WHEN DATEDIFF('day', {{ due_date }}, {{ payment_date }}) <= 30 THEN 'LATE_1_30'
                WHEN DATEDIFF('day', {{ due_date }}, {{ payment_date }}) <= 60 THEN 'LATE_31_60'
                WHEN DATEDIFF('day', {{ due_date }}, {{ payment_date }}) <= 90 THEN 'LATE_61_90'
                ELSE 'LATE_90_PLUS'
            END
        -- Unpaid scenarios
        ELSE
            CASE
                WHEN CURRENT_DATE() <= {{ due_date }} THEN 'UNPAID_CURRENT'
                WHEN DATEDIFF('day', {{ due_date }}, CURRENT_DATE()) <= 30 THEN 'UNPAID_1_30'
                WHEN DATEDIFF('day', {{ due_date }}, CURRENT_DATE()) <= 60 THEN 'UNPAID_31_60'
                WHEN DATEDIFF('day', {{ due_date }}, CURRENT_DATE()) <= 90 THEN 'UNPAID_61_90'
                ELSE 'UNPAID_90_PLUS'
            END
    END

{% endmacro %}


{% macro is_payment_at_risk(payment_date, due_date, risk_threshold_days=60) %}
{#-
    Flag payments at risk (unpaid and significantly overdue)
    
    Parameters:
        risk_threshold_days: Number of days past due to consider "at risk" (default: 60)
    
    Returns:
        TRUE if at risk, FALSE otherwise
-#}

    CASE
        WHEN {{ payment_date }} IS NULL 
             AND CURRENT_DATE() > DATEADD('day', {{ risk_threshold_days }}, {{ due_date }})
        THEN TRUE
        ELSE FALSE
    END

{% endmacro %}

