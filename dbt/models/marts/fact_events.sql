-- Event fact table (grain: one GDELT event). Foreign keys are computed with the
-- same md5_key macro / date_key formula as the dimensions, so they join exactly.
-- FKs are NULL when the source event lacks that actor/place (no dimension member).

select
    global_event_id as event_key,

    -- foreign keys
    cast(strftime(sql_date, '%Y%m%d') as integer)                        as date_key,
    {{ md5_key('actor1_code') }}                                         as actor1_key,
    {{ md5_key('actor2_code') }}                                         as actor2_key,
    {{ md5_key('coalesce(action_geo_feature_id, action_geo_fullname)') }} as action_geo_key,
    {{ md5_key('event_code') }}                                          as cameo_key,

    -- degenerate dimensions
    is_root_event,
    quad_class,
    source_url,

    -- measures
    goldstein_scale,
    num_mentions,
    num_sources,
    num_articles,
    avg_tone
from {{ ref('stg_events') }}
