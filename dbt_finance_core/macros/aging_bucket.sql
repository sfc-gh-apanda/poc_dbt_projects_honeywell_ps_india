{% macro aging_bucket(days_late_column) %}
/*
    Macro: aging_bucket
    
    Purpose: Calculate aging bucket based on days late
    
    Args:
        days_late_column: Column or expression containing days late value
    
    Returns:
        CASE statement returning aging bucket category
    
    Usage:
        {{ aging_bucket('days_late') }}
        {{ aging_bucket('datediff(day, due_date, current_date())') }}
    
    Note: Copied from dbt_foundation for standalone execution
*/

    case
        when {{ days_late_column }} <= 0 then 'CURRENT'
        when {{ days_late_column }} between 1 and 30 then '1-30'
        when {{ days_late_column }} between 31 and 60 then '31-60'
        when {{ days_late_column }} between 61 and 90 then '61-90'
        when {{ days_late_column }} between 91 and 120 then '91-120'
        when {{ days_late_column }} between 121 and 150 then '121-150'
        when {{ days_late_column }} between 151 and 180 then '151-180'
        when {{ days_late_column }} between 181 and 360 then '181-360'
        else '361+'
    end

{% endmacro %}

