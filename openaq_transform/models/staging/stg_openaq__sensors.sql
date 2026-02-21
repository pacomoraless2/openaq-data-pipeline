{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('openaq', 'raw_locations') }}
),

extracted AS (
    SELECT
        -- Surrogate Key: Unique identifier for each snapshot of a sensor
        {{ dbt_utils.generate_surrogate_key(['JSON_VALUE(sensor.id)', '_audit_logical_date']) }} AS sensor_record_id,

        -- Link to Location (Parent dimension)
        CAST(JSON_VALUE(data.id) AS INT64) AS location_id,
        
        -- Link to measurements (Natural Key)
        CAST(JSON_VALUE(sensor.id) AS INT64) AS sensor_id,
        
        -- Sensor metadata (parameter details)
        JSON_VALUE(sensor.name) AS sensor_name,
        CAST(JSON_VALUE(sensor.parameter.id) AS INT64) AS parameter_id,
        JSON_VALUE(sensor.parameter.name) AS parameter_name,
        JSON_VALUE(sensor.parameter.units) AS unit,
        
        -- Audit Metadata
        _audit_run_id,
        CAST(_audit_logical_date AS DATE) AS logical_date,
        CAST(_audit_extracted_at AS TIMESTAMP) AS extracted_at
        
    FROM source,
    UNNEST(JSON_QUERY_ARRAY(data.sensors)) AS sensor
),

deduplicated AS (
    SELECT *
    FROM extracted
    -- Keep only the most recently extracted record per sensor per logical snapshot
    QUALIFY ROW_NUMBER() OVER(PARTITION BY sensor_record_id ORDER BY extracted_at DESC) = 1
)

SELECT * FROM deduplicated