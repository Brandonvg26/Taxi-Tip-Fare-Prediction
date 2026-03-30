{{ config(materialized='view') }}

{{ config(materialized='view') }}

WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                tpep_pickup_datetime,
                pulocationid,
                dolocationid,
                fare_amount
            ORDER BY tpep_pickup_datetime
        ) AS row_num
    FROM {{ source('raw', 'raw_sales') }}
    WHERE
        fare_amount       > 0
        AND trip_distance  > 0
        AND passenger_count BETWEEN 1 AND 8
        AND tpep_pickup_datetime >= '2025-09-01'
        AND tpep_pickup_datetime <  '2025-11-01'
        AND payment_type IN (1, 2, 3, 4, 5, 6)
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'tpep_pickup_datetime',
        'pulocationid',
        'dolocationid',
        'fare_amount'
    ]) }}                         AS trip_id,
    tpep_pickup_datetime          AS pickup_at,
    tpep_dropoff_datetime         AS dropoff_at,
    passenger_count,
    trip_distance,
    pulocationid                  AS pickup_location_id,
    dolocationid                  AS dropoff_location_id,
    ratecodeid,
    fare_amount,
    tip_amount,
    payment_type

FROM deduplicated
WHERE row_num = 1