{{
    config(
        materialized = 'table'
    )
}}

with year_quarter_total_revenue as 
(
  select
  service_type,
  extract(year FROM pickup_datetime) AS year,
  extract(quarter FROM pickup_datetime) as quarter,
  sum(total_amount) as total_revenue
  from {{ref('fact_trips')}}
  where extract(year FROM pickup_datetime) IN (2019, 2020)
  group by 1, 2,3
),
prev_year_quarter_revenue_cte as 
(
  select
  service_type,
  year,
  quarter,
  total_revenue,
  (lag (total_revenue) over(partition by service_type, quarter order by year)) as prev_year_quarter_revenue
  from year_quarter_total_revenue
)
select
service_type,
year,
quarter,
total_revenue,
prev_year_quarter_revenue,
(total_revenue - prev_year_quarter_revenue) * 100.0 / NULLIF(prev_year_quarter_revenue, 0) as yoy_revenue_growth
from prev_year_quarter_revenue_cte
order by 1, 3, 2