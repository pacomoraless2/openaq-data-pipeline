{{ config(
    materialized='view',
    tags=['silver', 'intermediate']
) }}

WITH stg_measurements AS (
    SELECT * FROM {{ ref('stg_openaq__measurements') }}
),

/* Data Validation & Quality Assurance: 
   Filter records based on OpenAQ sensor reliability and physical feasibility.
   Domain-specific constraints are applied based on the parameter_id.
*/
cleaned_measurements AS (
    SELECT 
        *
    FROM stg_measurements
    WHERE 
        -- Filter out records flagged by the OpenAQ API as having questionable data integrity
        has_flags = FALSE
        
        -- Apply physical thresholds depending on the measurement type
        AND CASE
            -- Temperature (ID:100): Allow negative values, restricted to realistic Earth temperatures (Celsius safe limits)
            WHEN parameter_id = 100 THEN measurement_value BETWEEN -80 AND 60
            
            -- Wind Direction (ID:22): Must be a valid compass degree
            WHEN parameter_id = 22 THEN measurement_value BETWEEN 0 AND 360
            
            -- Relative Humidity (ID:98): Must be a percentage
            WHEN parameter_id = 98 THEN measurement_value BETWEEN 0 AND 100
            
            -- Wind Speed and pollutants (PM2.5, NO2, O3, etc.): Cannot be negative
            ELSE measurement_value >= 0
        END
)

SELECT * FROM cleaned_measurements