{{ config(
    materialized='incremental',
    unique_key='air_quality_record_id',
    partition_by={
      "field": "measurement_hour_utc",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['country_code', 'location_id'],
    tags=['gold', 'mart', 'air_quality']
) }}

WITH measurements AS (
    SELECT * FROM {{ ref('int_valid_measurements') }}
    
    {% if is_incremental() %}
        -- Process only new or late-arriving measurements based on the latest hourly boundary
        WHERE measured_from_utc >= (SELECT MAX(measurement_hour_utc) FROM {{ this }})
    {% endif %}
),

sensors_enriched AS (
    SELECT * FROM {{ ref('int_sensors_enriched') }}
),

/* 1. Denormalization: 
  Join clean measurements with geographical context and define time boundaries.
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

/* 2. Pivoting & Calendar Extraction for Air Quality:
  Transform to a wide fact table.
  Column names include their unit of measurement 
  OpenAQ tracks the same pollutant in multiple units
*/
air_quality_pivot AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['location_id', 'measurement_hour_utc']) }} AS air_quality_record_id,
        
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

        -- Particulate Matter (PM)
        ROUND(AVG(CASE WHEN parameter_id = 1 THEN measurement_value END), 2) AS pm10_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 2 THEN measurement_value END), 2) AS pm25_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 19 THEN measurement_value END), 2) AS pm1_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 125 THEN measurement_value END), 2) AS um003_particles_cm3,

        -- Gases (Multiple units)
        ROUND(AVG(CASE WHEN parameter_id = 3 THEN measurement_value END), 2) AS o3_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 4 THEN measurement_value END), 2) AS co_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 102 THEN measurement_value END), 2) AS co_ppb,
        ROUND(AVG(CASE WHEN parameter_id = 5 THEN measurement_value END), 2) AS no2_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 15 THEN measurement_value END), 2) AS no2_ppb,
        ROUND(AVG(CASE WHEN parameter_id = 6 THEN measurement_value END), 2) AS so2_ugm3,
        ROUND(AVG(CASE WHEN parameter_id = 101 THEN measurement_value END), 2) AS so2_ppb,
        ROUND(AVG(CASE WHEN parameter_id = 23 THEN measurement_value END), 2) AS nox_ppb,
        ROUND(AVG(CASE WHEN parameter_id = 24 THEN measurement_value END), 2) AS no_ppb,
        ROUND(AVG(CASE WHEN parameter_id = 19843 THEN measurement_value END), 2) AS no_ugm3

    FROM joined_data
    GROUP BY 
        location_id,
        location_name,
        country_code,
        latitude,
        longitude,
        measurement_hour_utc
)

SELECT * FROM air_quality_pivot