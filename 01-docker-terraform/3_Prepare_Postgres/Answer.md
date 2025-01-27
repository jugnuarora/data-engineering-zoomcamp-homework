Step 1:
Create docker-compose.yaml (copy from question 2) (you may include networks as the service as I have used it as de-homework-network)

Step 2:
Create ingest_data.py (copy from the course) (Remember to change tpep_ to lpep_ for lpep_pickup_datetime & lpep_dropoff_datetime)

Step 3:
Create Dockerfile (copy from the course)

Step 4:
docker build -t taxi_ingest:v003 .
(version number depends on your choice)

Step 5:
docker-compose up -d
(Run postgres and pgadmin as present in docker-compose. -d is to execute it in detached mode so that we can reuse the terminal)

Step 6:
Run taxi_ingest with below code:
```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

docker run -it \
  --network=de-homework-network \
  taxi_ingest:v003 \
    --user=postgres \
    --password=postgres \
    --host=db \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}
```

Step 7:
Created upload_data.ipynb for ingesting the downloaded (using wget) zones csv file 
Please note that for engine creation, it will now differ.
```
engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')
```
