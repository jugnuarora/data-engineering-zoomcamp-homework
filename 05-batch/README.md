__STEP 1:__ `Question1`- Run `spark-shell`in terminal. execute `spark.version`. 
`Answer: 3.5.4`

__STEP 2:__ `Question 2`-Execute `data_read_to_parquet.ipynb`to get the data and upload to data folder as parquet after repartitioning to 4. 
```
df_yellow.repartition(4).write\
    .parquet("data/pq/repartitioned")
```

`Answer: 22M (nearest 25M)` 

__STEP 3:__ `Question 3`- Execute the SQL
```
df_count_15_oct = spark.sql("""
SELECT
    count(*)
FROM
    yellow
where tpep_pickup_datetime >= '2024-10-15 00:00:00' and
      tpep_pickup_datetime < '2024-10-16 00:00:00'                    
"""                        
)

df_count_15_oct.show()
```
```Answer: 128893 nearest: 125,567```

__STEP 4:__ Question 4:
````
df_longest_trip = spark.sql(''' 
SELECT
    max(unix_timestamp(tpep_dropoff_datetime) - 
    unix_timestamp(tpep_pickup_datetime)) / 3600 AS longest_trip_hours 
FROM
    yellow
''')

df_longest_trip.show()
````

`Answer: 162.617 (162)`

__STEP 5:__ Question 5
`Answer: 4040`

__STEP 6:__ Question 6

````
least_frequent_pickup_zone = spark.sql('''
SELECT tz.Zone,
    COUNT(y.PULocationID) AS pickup_count
FROM
    yellow as y
INNER JOIN
    taxi_zone_table as tz
ON
    y.PULocationID = tz.LocationID
GROUP BY 
   tz.Zone
ORDER BY 
    pickup_count ASC
LIMIT 1
''')

least_frequent_pickup_zone.show()
`````

`Answer: Governor's Island`