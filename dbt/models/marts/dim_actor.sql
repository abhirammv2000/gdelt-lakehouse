-- Actor dimension. GDELT records two actors per event; we union both roles and
-- deduplicate on the CAMEO actor code, taking a representative set of attributes
-- (max() is deterministic and the attributes are near-constant per code).

with actors as (
    select
        actor1_code            as actor_code,
        actor1_name            as actor_name,
        actor1_country_code    as country_code,
        actor1_known_group_code as known_group_code,
        actor1_ethnic_code     as ethnic_code,
        actor1_religion_code   as religion_code,
        actor1_type_code       as type_code
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
),

deduped as (
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
)

select
    {{ md5_key('actor_code') }} as actor_key,
    actor_code,
    actor_name,
    country_code,
    known_group_code,
    ethnic_code,
    religion_code,
    type_code
from deduped
