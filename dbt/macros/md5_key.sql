{#
  Deterministic surrogate key from one natural-key expression.
  Wrapping it in a macro keeps fact FKs and dimension keys byte-for-byte
  identical (both sides call this), so relationship tests hold.
  NULL natural key -> NULL key (the row simply has no dimension member).

  Dispatches per warehouse so the same models run on DuckDB and BigQuery:
  DuckDB's md5() already returns a hex string. BigQuery's MD5() returns BYTES, so it
  needs to_hex(). Athena (Trino) also returns varbinary and only hashes varbinary, so
  the value goes through to_utf8() first.
#}
{% macro md5_key(expr) %}
    {{ return(adapter.dispatch('md5_key', 'gdelt_gold')(expr)) }}
{% endmacro %}

{% macro default__md5_key(expr) %}
    md5(cast({{ expr }} as varchar))
{% endmacro %}

{% macro bigquery__md5_key(expr) %}
    to_hex(md5(cast({{ expr }} as string)))
{% endmacro %}

{% macro athena__md5_key(expr) %}
    to_hex(md5(to_utf8(cast({{ expr }} as varchar))))
{% endmacro %}
