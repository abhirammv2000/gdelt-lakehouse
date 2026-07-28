-- CAMEO event-type dimension: one row per distinct event code, enriched with the
-- human-readable root category and quad-class label from the cameo_event_root seed.

with events as (
    select distinct
        event_code,
        event_base_code,
        event_root_code,
        quad_class
    from {{ ref('stg_events') }}
    where event_code is not null
)

select
    {{ md5_key('e.event_code') }} as cameo_key,
    e.event_code,
    e.event_base_code,
    e.event_root_code,
    r.root_description,
    e.quad_class,
    r.quad_class_name
from events e
left join {{ ref('cameo_event_root') }} r
    -- normalise width so "1" and "01" both match the seed's "01".."20"
    on lpad(e.event_root_code, 2, '0') = r.event_root_code
