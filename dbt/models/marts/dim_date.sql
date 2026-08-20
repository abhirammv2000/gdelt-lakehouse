-- Date dimension, one row per distinct event date present in the data.
-- date_key is the yyyymmdd integer, matching fact_events.date_key.

with dates as (
    select distinct sql_date
    from {{ ref('stg_events') }}
    where sql_date is not null
)

select
    {{ yyyymmdd('sql_date') }}         as date_key,
    sql_date                           as full_date,
    extract(year from sql_date)        as year,
    extract(quarter from sql_date)     as quarter,
    extract(month from sql_date)       as month,
    {{ month_name('sql_date') }}       as month_name,
    extract(day from sql_date)         as day_of_month,
    {{ day_of_week('sql_date') }}      as day_of_week,   -- 0=Sunday
    {{ day_name('sql_date') }}         as day_name,
    {{ day_of_week('sql_date') }} in (0, 6) as is_weekend
from dates
