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
