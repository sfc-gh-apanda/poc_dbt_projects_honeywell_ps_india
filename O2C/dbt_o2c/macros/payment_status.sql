{% macro payment_status(payment_date, due_date) %}
/*
    Classify payment timing status
    
    Returns:
        - 'ON_TIME' if paid before or on due date
        - 'LATE' if paid after due date
        - 'OVERDUE' if not paid and past due date
        - 'CURRENT' if not paid and not past due date
    
    Usage:
        {{ payment_status('payment_date', 'due_date') }}
*/

    case
        when {{ payment_date }} is not null and {{ payment_date }} <= {{ due_date }} then 'ON_TIME'
        when {{ payment_date }} is not null and {{ payment_date }} > {{ due_date }} then 'LATE'
        when {{ payment_date }} is null and current_date() > {{ due_date }} then 'OVERDUE'
        else 'CURRENT'
    end

{% endmacro %}

