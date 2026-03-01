{{ config(
    materialized='view',
    tags=['silver', 'intermediate']
) }}

WITH stg_measurements AS (
    SELECT * FROM {{ ref('stg_openaq__measurements') }}
),

/* Data Validation & Quality Assurance: 
   Filter records based on OpenAQ sensor reliability and physical feasibility.
*/
cleaned_measurements AS (
    SELECT 
        *
    FROM stg_measurements
    WHERE 
        -- Exclude anomalous negative readings often caused by sensor calibration drift
        measurement_value >= 0 
        
        -- Filter out records flagged by the OpenAQ API as having questionable data integrity
        AND has_flags = FALSE
)

SELECT * FROM cleaned_measurements