-- Event fact table (grain: one GDELT event). Foreign keys are computed with the
-- same md5_key macro / date_key formula as the dimensions, so they join exactly.
-- FKs are NULL when the source event lacks that actor/place (no dimension member).
--
-- Incremental: only events added since the last load are processed, and
-- delete+insert on event_key upserts re-published events (silver already keeps the
-- most-recent copy). date_added is the high-water mark.

{{
    config(
        materialized="incremental",
        unique_key="event_key",
        incremental_strategy="delete+insert",
    )
}}

with source as (
    select * from {{ ref('stg_events') }}
    {% if is_incremental() %}
    where date_added > (select coalesce(max(date_added), timestamp '1900-01-01') from {{ this }})
    {% endif %}
)

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
    avg_tone,

    -- audit / incremental high-water mark
    date_added
from source
