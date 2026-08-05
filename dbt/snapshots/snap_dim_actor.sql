{#
  Slowly Changing Dimension Type 2 for actors.

  dbt compares int_actor_attributes to the stored snapshot on each run; when any
  of check_cols changes for an actor_code, it closes the current row
  (dbt_valid_to = now) and inserts a new version. Full attribute history is
  retained; dim_actor exposes the current version (dbt_valid_to is null).
#}
{% snapshot snap_dim_actor %}

{{
    config(
        target_schema="snapshots",
        unique_key="actor_code",
        strategy="check",
        check_cols=[
            "actor_name",
            "country_code",
            "known_group_code",
            "ethnic_code",
            "religion_code",
            "type_code",
        ],
    )
}}

select * from {{ ref('int_actor_attributes') }}

{% endsnapshot %}
