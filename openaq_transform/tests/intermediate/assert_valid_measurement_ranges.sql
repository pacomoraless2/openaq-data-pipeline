/*
Physical feasibility of measurements.

It returns any rows that break these rules (a passing test returns 0 rows).
*/

WITH validation AS (
    SELECT 
        sensor_id,
        parameter_id,
        measurement_value,
        has_flags
    FROM {{ ref('int_valid_measurements') }}
)

SELECT *
FROM validation
WHERE 
    -- 1. Fail if there are any records with source data quality warnings
    has_flags = TRUE
    
    -- 2. Fail if measurements violate physical constraints (inverse of the model logic)
    OR (
        CASE
            -- Temperature (ID 100): Fail if outside the -80 to 60 Celsius range
            WHEN parameter_id = 100 THEN measurement_value < -80 OR measurement_value > 60
            
            -- Wind Direction (ID 22): Fail if outside the 0 to 360 degrees range
            WHEN parameter_id = 22 THEN measurement_value < 0 OR measurement_value > 360
            
            -- Relative Humidity (ID 98): Fail if outside the 0 to 100 percentage range
            WHEN parameter_id = 98 THEN measurement_value < 0 OR measurement_value > 100
            
            -- All other parameters (Pollutants, Wind Speed, etc.): Fail if negative
            ELSE measurement_value < 0
        END
    )