{% macro calculate_dso(order_date, payment_date) %}
/*
    Calculate Days Sales Outstanding (DSO)
    
    Returns the number of days between order and payment.
    Returns NULL if payment_date is NULL.
    
    Usage:
        {{ calculate_dso('order_date', 'payment_date') }}
*/

    datediff('day', {{ order_date }}, {{ payment_date }})

{% endmacro %}

