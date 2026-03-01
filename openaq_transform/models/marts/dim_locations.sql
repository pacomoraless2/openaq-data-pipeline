{{ config(
    materialized='table',
    cluster_by=['country_code'],
    tags=['gold', 'mart', 'dimension']
) }}

WITH sensors_enriched AS (
    SELECT * FROM {{ ref('int_sensors_enriched') }}
),

/* Deduplication: 
  Extract strictly unique location metadata to form a true Dimension table.
  This removes the sensor-level grain from the intermediate model.
*/
unique_locations AS (
    SELECT DISTINCT
        location_id,
        location_name,
        country_code,
        country_name,
        latitude,
        longitude
    FROM sensors_enriched
    WHERE location_id IS NOT NULL
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS last_updated_utc 
FROM unique_locations