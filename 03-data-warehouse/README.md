__STEP 1:__ Create a new project in GCP. I have created it with the name DE-Zoomcamp-Warehouse

__STEP 2:__ Create project in google cloud. IAM - create service account.  Select cloud storage and set as admin. Select bigquery and set as Admin. 

__STEP 3:__ Download the parquet files

__STEP 4:__ Upload to GCS

OR Generate Key and use this JSON as gcs.json. Run load_yellow_taxi_data.ipynb.

__STEP 5:__ Create an external table using the Yellow Taxi Trip Records in GCS.
````
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-warehouse.zoomcamp_homework.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://jugnu-arora-de-zoomcamp-warehouse/yellow_tripdata_2024-*.parquet']
);
````

Check yellow trip data

````
SELECT * FROM de-zoomcamp-warehouse.zoomcamp_homework.external_yellow_tripdata limit 10;
````
__STEP 6:__ `Question 1`

Create a non partitioned table from external table

````
CREATE OR REPLACE TABLE de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_non_partitioned AS
SELECT * FROM de-zoomcamp-warehouse.zoomcamp_homework.external_yellow_tripdata;
`````

`Answer: Count of Records: 20,332,093`

--STEP 7:__ `Question 2`

````
SELECT COUNT(DISTINCT PULocationID)
FROM de-zoomcamp-warehouse.zoomcamp_homework.external_yellow_tripdata;
`````

-- Estimated is 0 Bytes

````
SELECT COUNT(DISTINCT PULocationID)
FROM de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_non_partitioned;
`````

--Estimated is 155.12 MB

`Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table`

__STEP 8:__ `Question 3`

````
SELECT PULocationID
FROM de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_non_partitioned;
````

-- Estimated 155.12 MB

````
SELECT PULocationID, DOLocationID
FROM de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_non_partitioned;
````

-- Estimated 310.24

`Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`

__STEP 9:__ `Question 4`

````
SELECT COUNT(*)
FROM de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;
````

`Answer: 8333`

__STEP 10:__ `Question 5`

````
CREATE OR REPLACE TABLE de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_partitioned_clustered
PARTITION BY
  DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-zoomcamp-warehouse.zoomcamp_homework.external_yellow_tripdata;
````

`Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID`

__STEP 11:__ `Question 6`

````
SELECT DISTINCT VendorID
FROM de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
````
--310.24 MB estimated

````
SELECT DISTINCT VendorID
FROM de-zoomcamp-warehouse.zoomcamp_homework.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
````
-- 26.84 MB estimated

`Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`

__STEP 12:__ `Question 7`
`Answer: GCP Bucket`

__STEP 13:__ `Question 8`

`Answer: False`
-- Too many clusters can reduce efficiency, as BigQuery may need to scan many small partitions instead of optimizing larger ones.
-- If your queries don't frequently filter or order by clustered columns, clustering may not provide benefits and could increase storage costs.