{{
    config(
        materialized = 'table'
    )
}}

with fhv_tripdata as
(
    select *
    from {{ ref ('stg_fhv')}}
),
dim_zones as
(
    select *
    from {{ ref('dim_zones')}}
    where borough != 'Unkown'
)
select
    extract(year from fhv.pickup_datetime) as year,
    extract(month from fhv.pickup_datetime) as month,
    fhv.pickup_datetime,
    fhv.dropoff_datetime,
    timestamp_diff(fhv.dropoff_datetime, fhv.pickup_datetime, second)as trip_duration_sec,
    fhv.pulocationid as pickup_locationid,
    pickup_zone.zone as pickup_zone, 
    fhv.dolocationid as dropoff_locationid,
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata as fhv
left join dim_zones as pickup_zone
on fhv.PUlocationID = pickup_zone.locationid
left join dim_zones as dropoff_zone
on fhv.DOlocationID = dropoff_zone.locationid