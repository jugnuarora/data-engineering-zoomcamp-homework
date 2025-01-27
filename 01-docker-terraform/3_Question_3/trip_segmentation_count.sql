SELECT 
  CASE
    WHEN trip_distance <= 1 THEN 'Up to 1 mile'
    WHEN trip_distance > 1 AND trip_distance <= 3 THEN 'In between 1 (exclusive) and 3 miles (inclusive)'
    WHEN trip_distance > 3 AND trip_distance <= 7 THEN 'In between 3 (exclusive) and 7 miles (inclusive)'
    WHEN trip_distance > 7 AND trip_distance <= 10 THEN 'In between 7 (exclusive) and 10 miles (inclusive)'
    ELSE 'Over 10 miles'
  END AS distance,
  COUNT(*) AS trip_count
FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime) >= DATE '2019-10-01'
  AND DATE(lpep_pickup_datetime) < DATE '2019-11-01'
  AND DATE(lpep_dropoff_datetime) >= DATE '2019-10-01'
  AND DATE(lpep_dropoff_datetime) < DATE '2019-11-01'
GROUP BY distance
ORDER BY trip_count DESC;
