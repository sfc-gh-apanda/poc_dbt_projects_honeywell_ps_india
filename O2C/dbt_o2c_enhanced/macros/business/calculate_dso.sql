{% macro calculate_dso(order_date, payment_date) %}
{#-
================================================================================
    CALCULATE DAYS SALES OUTSTANDING (DSO)
================================================================================

    Purpose:
        Calculate the number of days between order and payment.
        Standardizes DSO calculation across all models.

    Parameters:
        order_date: Column name for order date
        payment_date: Column name for payment date

    Returns:
        Number of days between order and payment.
        Returns NULL if payment_date is NULL (unpaid).

    Usage:
        {{ calculate_dso('order_date', 'payment_date') }}
        
    Example Output:
        - Order: 2024-01-01, Payment: 2024-01-15 → 14
        - Order: 2024-01-01, Payment: NULL → NULL

================================================================================
-#}

    DATEDIFF('day', {{ order_date }}, {{ payment_date }})

{% endmacro %}


{% macro calculate_invoice_dso(invoice_date, payment_date) %}
{#-
    Calculate DSO from invoice date (alternative metric)
-#}

    DATEDIFF('day', {{ invoice_date }}, {{ payment_date }})

{% endmacro %}


{% macro calculate_days_past_due(due_date, reference_date='CURRENT_DATE()') %}
{#-
    Calculate days past due date
    
    Returns:
        - Positive number if past due
        - Negative number if not yet due
        - 0 if due today
-#}

    DATEDIFF('day', {{ due_date }}, {{ reference_date }})

{% endmacro %}

