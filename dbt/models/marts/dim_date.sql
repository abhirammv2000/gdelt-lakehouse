-- Date dimension, one row per distinct event date present in the data.
-- date_key is the yyyymmdd integer, matching fact_events.date_key.

with dates as (
    select distinct sql_date
    from {{ ref('stg_events') }}
    where sql_date is not null
)

select
    cast(strftime(sql_date, '%Y%m%d') as integer) as date_key,
    sql_date                                       as full_date,
    year(sql_date)                                 as year,
    quarter(sql_date)                              as quarter,
    month(sql_date)                                as month,
    monthname(sql_date)                            as month_name,
    day(sql_date)                                  as day_of_month,
    dayofweek(sql_date)                            as day_of_week,   -- 0=Sunday
    dayname(sql_date)                              as day_name,
    dayofweek(sql_date) in (0, 6)                  as is_weekend
from dates
