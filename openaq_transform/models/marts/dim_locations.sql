{{ config(
    materialized='table',
    cluster_by=['country_code'],
    tags=['gold', 'mart', 'dimension']
) }}

WITH sensors_enriched AS (
    SELECT * FROM {{ ref('int_sensors_enriched') }}
),

/* Slowly Changing Dimension (SCD Type 1) logic: 
   Extract the most recent snapshot for each location to form a true Dimension table.
   This removes the sensor-level grain and prevents row explosion if metadata changes.
*/
latest_locations AS (
    SELECT
        location_id,
        location_name,
        country_code,
        country_name,
        latitude,
        longitude
    FROM sensors_enriched
    WHERE location_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY location_id ORDER BY logical_date DESC) = 1
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS last_updated_utc 
FROM latest_locations