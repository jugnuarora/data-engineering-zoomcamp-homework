{{
    config(
        materialized = 'table'
    )
}}

with filtered_data as
(
select service_type, extract(year from pickup_datetime) as year, extract(month from pickup_datetime) as month, fare_amount
from {{ref('fact_trips')}}
where fare_amount >0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit card')
),
selected_year_month as
(
select *
from filtered_data
where year = 2020 and month = 4
)
select distinct service_type,
percentile_cont(fare_amount, 0.97 ) over (partition by service_type) as p97,
percentile_cont(fare_amount, 0.95 ) over (partition by service_type) as p95,
percentile_cont(fare_amount, 0.90 ) over (partition by service_type) as p90
from selected_year_month