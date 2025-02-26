# Homework 

The complete dbt repository is in folder [homework](./homework/)


__Question 1__

Create environment variables and give the given values.
In schema.yml, under sources past the given code. 
In test.sql, past the given code. Select the code and do compile. The `answer` will be
````
select * 
from `myproject`.`raw_nyc_tripdata`.`ext_green_taxi`
````
Reason:
  In sources.yml, the database and schema are specified using env_var(), which means dbt will first look for the environment variable.
  If the environment variable exists, dbt uses its value.
  If the environment variable is not set, dbt uses the default value specified in env_var().
  database resolves to myproject (because the environment variable is set).
  schema resolves to raw_nyc_tripdata (because DBT_BIGQUERY_SOURCE_DATASET is not set, so it defaults to raw_nyc_tripdata)

__Question 2__

Create Environment Variable DBT_DAYS_BACK and for development set it 7.
Create question2.sql in core as:
```
select *
from {{ ref('fact_trips') }}
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
where pickup_datetime >= TIMESTAMP(CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DBT_DAYS_BACK", "30")) }}' DAY)
```
Try running 
`dbt build --select question2 --vars '{'days_back': 9}'`
It will compile above code to:
````
select *
from `dbt-homework-451708`.`dbt_homework_core`.`fact_trips`
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
where pickup_datetime >= TIMESTAMP(CURRENT_DATE - INTERVAL '9' DAY)
````
suggesting the override of command line variable

Try running 
`dbt build --select question2`

It will compile the code to:
````
select *
from `dbt-homework-451708`.`dbt_homework_core`.`fact_trips`
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
where pickup_datetime >= TIMESTAMP(CURRENT_DATE - INTERVAL '7' DAY)
````
suggesting the override of environment variable

Next, update the environment variable to none and run
`dbt build --select question2`

It will compile to:
````
select *
from `dbt-homework-451708`.`dbt_homework_core`.`fact_trips`
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
where pickup_datetime >= TIMESTAMP(CURRENT_DATE - INTERVAL '30' DAY)
````

Thus the answer is:
````
where pickup_datetime >= TIMESTAMP(CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DBT_DAYS_BACK", "30")) }}' DAY)
````

__Question 3:__

Since only taxi_zone_lookup is materialized and nothing else, when we would run `dbt run --select models/staging/+`,
it will execute stg_green_tripdata and stg_yellow_tripdata since they are in staging. But when it will go to execute further in core which are referring to these 2 tables, it will try to execute dim_taxi_trips. But this also needs dim_zone_lookup which is not materialized and thus the model will fail resulting in non-execution of fct_taxi_monthly_zone_revenue.

__Question 4:__

All except number 2 are true.

For core models:
resolve_schema_for('core') → env_var('DBT_BIGQUERY_TARGET_DATASET')
This means DBT_BIGQUERY_TARGET_DATASET must be set, or the macro will fail.

For stg or other models (non-core):
resolve_schema_for('stg') → env_var('DBT_BIGQUERY_STAGING_DATASET', env_var('DBT_BIGQUERY_TARGET_DATASET'))
This means DBT_BIGQUERY_STAGING_DATASET is optional (if not set, it falls back to DBT_BIGQUERY_TARGET_DATASET).
Evaluating the Given Statements
1. "Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile."
`True`. Since DBT_BIGQUERY_TARGET_DATASET is always referenced (directly for core, and as a fallback for others), if it's missing, dbt will fail.

2. "Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile."
`False`. The macro provides a fallback to DBT_BIGQUERY_TARGET_DATASET if DBT_BIGQUERY_STAGING_DATASET is not set. It will still compile.

3. "When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET."
`True`. resolve_schema_for('core') explicitly returns DBT_BIGQUERY_TARGET_DATASET.

4. "When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET."
`True`. resolve_schema_for('stg') tries to use DBT_BIGQUERY_STAGING_DATASET, but if it's missing, it falls back to DBT_BIGQUERY_TARGET_DATASET.

5. "When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET."
`True`. The same logic applies as in the previous statement—any non-core model uses DBT_BIGQUERY_STAGING_DATASET if available, or DBT_BIGQUERY_TARGET_DATASET otherwise.

__Question 5:__
The model is present [here](./homework/models/core/fct_taxi_trips_quarterly_revenue.sql).

The `answer` is:
```
green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
```

__Question 6:__
The model is present [here](./homework/models/core/fct_taxi_trips_monthly_fare_p95.sql).

The `answer`is:
````
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
````

__Question 7:__
The model is present [here](./homework/models/core/fct_fhv_monthly_zone_traveltime_p90.sql).

The above model generates the table for all the trips originating from Newark Airport, Yorkville East and SoHo. To only get the ones with second highest trip duration, below query would work:

````
with row_number_assigned as
(
SELECT *, 
row_number() over(partition by pickup_zone order by trip_duration_sec_p90 desc) as rn
FROM `dbt_homework_core.fct_fhv_monthly_zone_traveltime_p90`
)
select *
from row_number_assigned
where rn=2
````

The `answer`is:

`LaGuardia Airport, Chinatown, Garment District`

# Pre-Homework set-up
__Step 1:__ Create a GCP Project `DBT-Homework`

__Step 2:__ IAM - create service account.  Select cloud storage and set as admin. Select bigquery and set as Admin. No need to create buckets manually here as it will be created using kestra. 

__Step 3:__ Go to service account and manage keys. Generate JSON key. I normally save as `gcs.json`. Make sure *.json is in .gitignore

__Step 4:__ Create `docker-compose.yml` to run Kestra image locally.

__Step 5:__ Execute `docker-compose up -d`. In case it complains of already running container, kill it using `docker rm -f <container name>``

__Step 6:__ Open Kestra in web-browser using localhost:(whatever local mapping you have to the port)

__Step 7:__ Create first flow `01_gcp_kv.yaml`.  

__Step 8:__ Make sure that KV pairs are correct by going to namespaces and then to KV Store

__Step 9:__ Create and execute flow `02_gcp_setup.yaml`

__Step 10:__ Create `03_gcp_taxi.yaml`. Execute backfill. It was earlier not getting executed for long timeframe. It seems to be a bug. Current fix is to remove timezone from triggers in the script. More on this bug: https://github.com/kestra-io/kestra/issues/7227

__Step 11:__ Create final tables in warehouse:
````
CREATE OR REPLACE EXTERNAL TABLE `dbt-homework-451708.dbt_homework.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://jugnu_arora_dbt_homework/green_tripdata_20*.csv']);

SELECT * FROM dbt-homework-451708.dbt_homework.external_green_tripdata limit 3;

CREATE OR REPLACE TABLE dbt-homework-451708.dbt_homework.green_tripdata
PARTITION BY
  DATE(lpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dbt-homework-451708.dbt_homework.external_green_tripdata;
````