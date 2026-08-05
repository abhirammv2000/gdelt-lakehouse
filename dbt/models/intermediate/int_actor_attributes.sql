-- One row per CAMEO actor code with representative attributes, unioned across both
-- actor roles. Materialized as a table so the SCD2 snapshot (snap_dim_actor) has a
-- stable relation to diff against on each run.

{{ config(materialized="table") }}

with actors as (
    select
        actor1_code             as actor_code,
        actor1_name             as actor_name,
        actor1_country_code     as country_code,
        actor1_known_group_code as known_group_code,
        actor1_ethnic_code      as ethnic_code,
        actor1_religion_code    as religion_code,
        actor1_type_code        as type_code
    from {{ ref('stg_events') }}
    where actor1_code is not null

    union all

    select
        actor2_code,
        actor2_name,
        actor2_country_code,
        actor2_known_group_code,
        actor2_ethnic_code,
        actor2_religion_code,
        actor2_type_code
    from {{ ref('stg_events') }}
    where actor2_code is not null
)

select
    actor_code,
    max(actor_name)       as actor_name,
    max(country_code)     as country_code,
    max(known_group_code) as known_group_code,
    max(ethnic_code)      as ethnic_code,
    max(religion_code)    as religion_code,
    max(type_code)        as type_code
from actors
group by actor_code
