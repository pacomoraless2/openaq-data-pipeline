{{ config(
    materialized='table',
    partition_by={
      "field": "measurement_hour_utc",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['country_code', 'location_id'],
    tags=['gold', 'mart', 'weather']
) }}

WITH measurements AS (
    SELECT * FROM {{ ref('int_valid_measurements') }}
),

sensors_enriched AS (
    SELECT * FROM {{ ref('int_sensors_enriched') }}
),

/* 1. Denormalization & Time Normalization (Downsampling): 
  Join clean measurements with geographical context.
  We truncate the timestamp to the HOUR. This normalizes mixed granularities
*/
joined_data AS (
    SELECT
        s.location_id,
        s.location_name,
        s.country_code,
        s.latitude,
        s.longitude,
        -- Force all timestamps to snap to the beginning of the hour
        TIMESTAMP_TRUNC(m.measured_from_utc, HOUR) AS measurement_hour_utc,
        m.parameter_id,
        m.measurement_value
    FROM measurements m
    JOIN sensors_enriched s
        ON m.sensor_id = s.sensor_id
),

/* 2. Pivoting & Aggregation:
  Transform to a wide fact table and extract BI-friendly time dimensions 
  based on the standardized hourly boundary (measurement_hour_utc).
*/
weather_pivot AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['location_id', 'measurement_hour_utc']) }} AS weather_record_id,
        
        location_id,
        location_name,
        country_code,
        latitude,
        longitude,
        measurement_hour_utc,

        -- Date/time
        EXTRACT(DATE FROM measurement_hour_utc) AS date_utc,
        EXTRACT(YEAR FROM measurement_hour_utc) AS year_utc,
        EXTRACT(MONTH FROM measurement_hour_utc) AS month_utc,
        EXTRACT(DAY FROM measurement_hour_utc) AS day_utc,
        EXTRACT(HOUR FROM measurement_hour_utc) AS hour_utc,

        -- Temperature: Pivot and aggregate
        ROUND(AVG(CASE WHEN parameter_id = 100 THEN measurement_value END), 2) AS temp_celsius,
        ROUND(AVG(CASE WHEN parameter_id = 100 THEN (measurement_value * 9/5) + 32 END), 2) AS temp_fahrenheit,

        -- Humidity
        ROUND(AVG(CASE WHEN parameter_id = 98 THEN measurement_value END), 2) AS humidity_pct,

        -- Wind
        ROUND(AVG(CASE WHEN parameter_id = 34 THEN measurement_value END), 2) AS wind_speed_ms,
        ROUND(AVG(CASE WHEN parameter_id = 22 THEN measurement_value END), 0) AS wind_direction_deg

    FROM joined_data
    GROUP BY 
        location_id,
        location_name,
        country_code,
        latitude,
        longitude,
        measurement_hour_utc
)

SELECT * FROM weather_pivot