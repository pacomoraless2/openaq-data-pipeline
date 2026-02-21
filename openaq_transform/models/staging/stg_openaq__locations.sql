{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('openaq', 'raw_locations') }}
),

extracted AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['JSON_VALUE(data.id)', '_audit_logical_date']) }} AS record_id,
        
        -- Main identifiers and attributes
        CAST(JSON_VALUE(data.id) AS INT64) AS location_id,
        JSON_VALUE(data.name) AS location_name,
        
        -- Geography 
        COALESCE(
            JSON_VALUE(data.locality), 
            JSON_VALUE(data.city),
            REPLACE(SPLIT(JSON_VALUE(data.timezone), '/')[SAFE_OFFSET(1)], '_', ' ')
        ) AS locality,
        JSON_VALUE(data.country.code) AS country_code,
        JSON_VALUE(data.country.name) AS country_name,
        
        -- Coordinates
        CAST(JSON_VALUE(data.coordinates.latitude) AS FLOAT64) AS latitude,
        CAST(JSON_VALUE(data.coordinates.longitude) AS FLOAT64) AS longitude,
        
        -- Station metadata
        JSON_VALUE(data.provider.name) AS provider_name,
        CAST(JSON_VALUE(data.isMobile) AS BOOL) AS is_mobile,
        CAST(JSON_VALUE(data.isMonitor) AS BOOL) AS is_monitor,
        JSON_VALUE(data.timezone) AS timezone,

        -- Audit Metadata
        _audit_run_id,
        CAST(_audit_logical_date AS DATE) AS logical_date,
        CAST(_audit_extracted_at AS TIMESTAMP) AS extracted_at
        
    FROM source
),

deduplicated AS (
    SELECT *
    FROM extracted
    -- Keep only the most recently extracted record per logical snapshot
    QUALIFY ROW_NUMBER() OVER(PARTITION BY record_id ORDER BY extracted_at DESC) = 1
)

SELECT * FROM deduplicated