{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('openaq', 'raw_measurements') }}
),

extracted AS (
    SELECT
        -- Surrogate Fact Key
        {{ dbt_utils.generate_surrogate_key([
            '_audit_sensor_id', 
            'JSON_VALUE(data.parameter.id)', 
            'JSON_VALUE(data.period.datetimeFrom.utc)'
        ]) }} AS measurement_id,

        -- Foreign Keys 
        CAST(_audit_sensor_id AS INT64) AS sensor_id,
        CAST(JSON_VALUE(data.parameter.id) AS INT64) AS parameter_id,
        
        -- The Fact
        CAST(JSON_VALUE(data.value) AS FLOAT64) AS measurement_value,
        
        -- Temporality
        CAST(JSON_VALUE(data.period.datetimeFrom.utc) AS TIMESTAMP) AS measured_from_utc,
        CAST(JSON_VALUE(data.period.datetimeTo.utc) AS TIMESTAMP) AS measured_to_utc,
        JSON_VALUE(data.period.interval) AS measurement_interval,
        
        -- Data Quality Flags
        CAST(JSON_VALUE(data.flagInfo.hasFlags) AS BOOL) AS has_flags,
        
        -- Audit & Partitioning
        _audit_run_id,
        CAST(_audit_logical_date AS DATE) AS logical_date,
        CAST(_audit_extracted_at AS TIMESTAMP) AS extracted_at
        
    FROM source
),

deduplicated AS (
    SELECT *
    FROM extracted
    -- Keep only the most recently extracted measurement event
    QUALIFY ROW_NUMBER() OVER(PARTITION BY measurement_id ORDER BY extracted_at DESC) = 1
)

SELECT * FROM deduplicated