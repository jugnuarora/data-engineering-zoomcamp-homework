{{
    config(
        materialized = 'table'
    )
}}

with trip_duration_calc as
(
    select *
    from {{ ref('dim_fhv_trips')}} as fhv
),
trip_duration_percentile_calc as
(
    select *,
    (percentile_cont(trip_duration_sec, 0.90) over(partition by year, month, pickup_locationid, dropoff_locationid)) as trip_duration_sec_p90
    from trip_duration_calc
    where pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
    and year = 2019
    and month = 11
)
select distinct
  pickup_zone, 
  pickup_locationid, 
  dropoff_zone, 
  dropoff_locationid, 
  trip_duration_sec_p90
from trip_duration_percentile_calc
order by pickup_zone, trip_duration_sec_p90 DESC