-- Geography dimension, built from each event's action location (where it
-- happened). Natural key prefers GDELT's geo feature id, falling back to the
-- full place name when no feature id is present.

with geo as (
    select
        coalesce(action_geo_feature_id, action_geo_fullname) as geo_natural_key,
        action_geo_type         as geo_type,
        action_geo_fullname     as geo_fullname,
        action_geo_country_code as country_code,
        action_geo_adm1_code    as adm1_code,
        action_geo_lat          as latitude,
        action_geo_long         as longitude,
        action_geo_feature_id   as feature_id
    from {{ ref('stg_events') }}
    where coalesce(action_geo_feature_id, action_geo_fullname) is not null
),

deduped as (
    select
        geo_natural_key,
        max(geo_type)     as geo_type,
        max(geo_fullname) as geo_fullname,
        max(country_code) as country_code,
        max(adm1_code)    as adm1_code,
        max(latitude)     as latitude,
        max(longitude)    as longitude,
        max(feature_id)   as feature_id
    from geo
    group by geo_natural_key
)

select
    {{ md5_key('geo_natural_key') }} as geo_key,
    geo_natural_key,
    geo_type,
    case geo_type
        when 1 then 'Country'
        when 2 then 'US State'
        when 3 then 'US City'
        when 4 then 'World City'
        when 5 then 'World State'
        else 'Unknown'
    end as geo_type_name,
    geo_fullname,
    country_code,
    adm1_code,
    latitude,
    longitude,
    feature_id
from deduped
