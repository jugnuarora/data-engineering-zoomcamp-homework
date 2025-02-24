{{
    config(
        materialized = 'view'
    )
}}

select *
from {{ source('staging', 'fhv_tripdata')}}
where dispatching_base_num is not null
and pulocationid is not null

{% if env_var('DBT_ENVIRONMENT') == 'development' %}

  limit 100

{% endif %}