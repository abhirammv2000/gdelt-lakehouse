-- Staging: a thin, renamed projection of the silver Iceberg table.
-- Silver is already typed and deduped, so staging just selects the columns the
-- gold star schema needs and gives the two actor blocks parallel names.

with source as (
    select * from {{ source('silver', 'events') }}
)

select
    global_event_id,
    sql_date,
    fraction_date,

    -- actor 1
    actor1_code,
    actor1_name,
    actor1_country_code,
    actor1_known_group_code,
    actor1_ethnic_code,
    actor1_religion1_code as actor1_religion_code,
    actor1_type1_code     as actor1_type_code,

    -- actor 2
    actor2_code,
    actor2_name,
    actor2_country_code,
    actor2_known_group_code,
    actor2_ethnic_code,
    actor2_religion1_code as actor2_religion_code,
    actor2_type1_code     as actor2_type_code,

    -- event action (CAMEO)
    is_root_event,
    event_code,
    event_base_code,
    event_root_code,
    quad_class,
    goldstein_scale,
    num_mentions,
    num_sources,
    num_articles,
    avg_tone,

    -- action geography (where the event took place)
    action_geo_type,
    action_geo_fullname,
    action_geo_country_code,
    action_geo_adm1_code,
    action_geo_lat,
    action_geo_long,
    action_geo_feature_id,

    date_added,
    source_url
from source
