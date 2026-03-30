{{ config(materialized='table')}}


SELECT
    trip_id,
    pickup_hour,
    pickup_dow,
    pickup_month,
    is_weekend,
    trip_duration_min,
    trip_distance,
    fare_per_mile,
    passenger_count,
    pickup_location_id,
    dropoff_location_id,
    ratecodeid,
    is_high_tip         -- ML target


FROM {{ ref('int_trips_features') }}


WHERE
    trip_duration_min BETWEEN 1 AND 180
    AND trip_distance  BETWEEN 0.1 AND 100
    AND fare_per_mile  BETWEEN 0.5 AND 50
