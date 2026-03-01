{{ config(
    materialized='view',
    tags=['silver', 'intermediate']
) }}

WITH sensors AS (
    SELECT * FROM {{ ref('stg_openaq__sensors') }}
),

locations AS (
    SELECT * FROM {{ ref('stg_openaq__locations') }}
),

/*
 Enrichment Layer: 
   Join sensor-level data with location metadata to provide geographical context
*/
enriched AS (
    SELECT
        s.sensor_record_id,
        s.sensor_id,
        s.parameter_id,
        s.parameter_name,
        s.unit,
        s.logical_date,
        l.location_id,
        l.location_name,
        l.country_code,
        l.country_name,
        l.latitude,
        l.longitude
    FROM sensors s
    LEFT JOIN locations l 
        ON s.location_id = l.location_id
        -- Maintain temporal alignment between sensors and location 
        AND s.logical_date = l.logical_date 
)

SELECT * FROM enriched