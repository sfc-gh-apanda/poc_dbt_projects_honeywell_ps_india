{% macro fiscal_period_str(date_column) %}
/*
    Macro: fiscal_period_str
    
    Purpose: Lookup fiscal period string (YYYY.MM) for a given date
    
    Args:
        date_column: Column or expression containing a date
    
    Returns:
        Scalar subquery returning fiscal_year_period_str
    
    Usage:
        {{ fiscal_period_str('posting_date') }}
*/

    (select fiscal_year_period_str
     from {{ source('corp_ref', 'time_fiscal_day') }}
     where fiscal_date_key_date = {{ date_column }})

{% endmacro %}


{% macro fiscal_period_int(date_column) %}
/*
    Macro: fiscal_period_int
    
    Purpose: Lookup fiscal period integer (YYYYMM) for a given date
    
    Args:
        date_column: Column or expression containing a date
    
    Returns:
        Scalar subquery returning fiscal_year_period_int
    
    Usage:
        {{ fiscal_period_int('posting_date') }}
*/

    (select fiscal_year_period_int
     from {{ source('corp_ref', 'time_fiscal_day') }}
     where fiscal_date_key_date = {{ date_column }})

{% endmacro %}

