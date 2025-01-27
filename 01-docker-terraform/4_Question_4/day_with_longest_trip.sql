SELECT DATE(lpep_pickup_datetime) AS pickup_date,
	trip_distance AS max_trip_distance
FROM green_taxi_trips
WHERE trip_distance = (SELECT max(trip_distance)
								FROM green_taxi_trips);