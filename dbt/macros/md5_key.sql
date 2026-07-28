{#
  Deterministic surrogate key from one natural-key expression.
  Wrapping it in a macro keeps fact FKs and dimension keys byte-for-byte
  identical (both sides call this), so relationship tests hold.
  NULL natural key -> NULL key (the row simply has no dimension member).
#}
{% macro md5_key(expr) %}
    md5(cast({{ expr }} as varchar))
{% endmacro %}
