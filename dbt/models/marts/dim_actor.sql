-- Actor dimension = the CURRENT version of the SCD2 snapshot (dbt_valid_to is
-- null). Full history lives in snap_dim_actor; most queries want "current", so
-- that's this dim. valid_from/valid_to are exposed for point-in-time joins.

with current_version as (
    select *
    from {{ ref('snap_dim_actor') }}
    where dbt_valid_to is null
)

select
    {{ md5_key('actor_code') }} as actor_key,
    actor_code,
    actor_name,
    country_code,
    known_group_code,
    ethnic_code,
    religion_code,
    type_code,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    true           as is_current
from current_version
