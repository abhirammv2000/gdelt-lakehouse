{#
  Date helpers dispatched per warehouse, so dim_date and fact_events run the
  same on DuckDB and BigQuery. DuckDB uses strftime/monthname/dayname and a
  0=Sunday day-of-week; BigQuery uses format_date and a 1=Sunday day-of-week.
  year/quarter/month/day are left inline in the models because EXTRACT(...)
  is identical on both engines.
#}

{# yyyymmdd integer surrogate key for a date (matches fact_events.date_key). #}
{% macro yyyymmdd(col) %}
    {{ return(adapter.dispatch('yyyymmdd', 'gdelt_gold')(col)) }}
{% endmacro %}
{% macro default__yyyymmdd(col) %}
    cast(strftime({{ col }}, '%Y%m%d') as integer)
{% endmacro %}
{% macro bigquery__yyyymmdd(col) %}
    cast(format_date('%Y%m%d', {{ col }}) as int64)
{% endmacro %}

{# Full month name, e.g. "August". #}
{% macro month_name(col) %}
    {{ return(adapter.dispatch('month_name', 'gdelt_gold')(col)) }}
{% endmacro %}
{% macro default__month_name(col) %}
    monthname({{ col }})
{% endmacro %}
{% macro bigquery__month_name(col) %}
    format_date('%B', {{ col }})
{% endmacro %}

{# Full weekday name, e.g. "Monday". #}
{% macro day_name(col) %}
    {{ return(adapter.dispatch('day_name', 'gdelt_gold')(col)) }}
{% endmacro %}
{% macro default__day_name(col) %}
    dayname({{ col }})
{% endmacro %}
{% macro bigquery__day_name(col) %}
    format_date('%A', {{ col }})
{% endmacro %}

{# Day of week normalized to 0=Sunday .. 6=Saturday on both engines. #}
{% macro day_of_week(col) %}
    {{ return(adapter.dispatch('day_of_week', 'gdelt_gold')(col)) }}
{% endmacro %}
{% macro default__day_of_week(col) %}
    extract(dow from {{ col }})
{% endmacro %}
{% macro bigquery__day_of_week(col) %}
    (extract(dayofweek from {{ col }}) - 1)
{% endmacro %}
