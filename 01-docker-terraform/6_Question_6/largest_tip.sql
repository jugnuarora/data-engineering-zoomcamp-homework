SELECT zdo."Zone", max(tip_amount)
FROM green_taxi_trips
LEFT JOIN zones zpu
ON "PULocationID" = zpu."LocationID"
LEFT JOIN zones zdo
oN "DOLocationID" = zdo."LocationID"
WHERE DATE_TRUNC('month',lpep_pickup_datetime) = MAKE_DATE(2019, 10, 01)
AND zpu."Zone" = 'East Harlem North'
GROUP BY zdo."Zone"
ORDER BY 2 DESC;
