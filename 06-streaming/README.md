__Step 1:__ Execute `docker compose up`

__Step 2:__ 

_Question 1:_ To get into redpanda container execute this:
`docker exec -it redpanda-1 bash` 
`rpk version`

Answer: 
```
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:59:41Z
OS/Arch:     linux/arm64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```


_Question 2:_ 
`rpk topic create green-trips`
Answer:
````
TOPIC        STATUS
green-trips  OK
````


_Question 3:_
Create a python script with the given code. The answer is `True`

_Question 4:_
Add import time and below code in main part of the program load_taxi_data.py:

```
    t0 = time.time()
    main()
    t1 = time.time()
    print(f'took {(t1 - t0):.2f} seconds')
```

Answer: `took 20.16 seconds`

_Question 5:_

Executed `session_job.py`. The idea is to find the pickup location ID and Drop off location ID having the longest streak or count of trips based on lpep_dropoff_datetime (within 5 minutes).

Once the data is in postgres, execute the below query in DBeaver:
````
SELECT * FROM longest_streaks ls 
ORDER BY 5 DESC
LIMIT 10;
````

The first entry is with __pu_location_id = 95 and do_location_id = 95 with streak count as 44.__

Let's verify it. 
I ran below query on the green_tripdata present in Bigquery:
````
select *
from `zoomcamp_code_along.green_tripdata`
where lpep_dropoff_datetime >= '2019-10-16 18:10:00' and lpep_dropoff_datetime < '2019-10-16 19:35:00'
and PULocationID = '95' and DOLocationID = '95'
ORDER BY lpep_dropoff_datetime;
````
And Checked for the number of trips that were within 5 minutes range and it was indeed 44.

__95 location id is Queens borough and Forest Hills zone.__



