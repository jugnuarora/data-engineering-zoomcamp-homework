select *
from {{ ref('fact_trips') }}
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY