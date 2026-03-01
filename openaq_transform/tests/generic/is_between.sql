-- This test allows us to check if the values are between min_value and max_value
-- Valid for longitude, latitude, measurements...

{% test is_between(model, column_name, min_value, max_value) %}

with validation as (
    select
        {{ column_name }} as field
    from {{ model }}
),

validation_errors as (
    select field
    from validation
    where field < {{ min_value }} 
       or field > {{ max_value }}
)

select *
from validation_errors

{% endtest %}