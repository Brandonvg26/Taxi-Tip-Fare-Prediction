{{ config(materialized='view') }}


SELECT
    trip_id,
    pickup_at,
    pickup_location_id,
    dropoff_location_id,
    ratecodeid,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,


    -- derived features
    EXTRACT(HOUR  FROM pickup_at)           AS pickup_hour,
    EXTRACT(DOW   FROM pickup_at)            AS pickup_dow,
    EXTRACT(MONTH FROM pickup_at)            AS pickup_month,
    CASE WHEN EXTRACT(DOW FROM pickup_at)
         IN (0,6) THEN 1 ELSE 0 END          AS is_weekend,
    EXTRACT(EPOCH FROM (dropoff_at - pickup_at))/60
                                             AS trip_duration_min,
    fare_amount / NULLIF(trip_distance, 0)   AS fare_per_mile,


    -- target variable
    -- only credit card trips (payment_type=1) have real tip data
    CASE WHEN payment_type = 1
              AND tip_amount > fare_amount * 0.2
         THEN 1 ELSE 0 END                   AS is_high_tip


FROM {{ ref('stg_yellow_trips') }}


WHERE payment_type = 1   -- exclude cash: tip_amount always 0 for cash