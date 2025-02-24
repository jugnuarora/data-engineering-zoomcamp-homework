{{
    config(
        materialized = 'view'
    )
}}

select 
    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_flag,
    Affiliated_base_number
from {{ source('staging', 'fhv_tripdata')}}
where dispatching_base_num is not null


{% if env_var('DBT_ENVIRONMENT') == 'development' %}

  limit 100

{% endif %}