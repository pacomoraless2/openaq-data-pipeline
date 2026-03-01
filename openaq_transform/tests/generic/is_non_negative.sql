{% test is_non_negative(model, column_name) %}

    select *
    from {{ model }}
    -- El test fallará si encuentra algún valor menor que cero
    where {{ column_name }} < 0

{% endtest %}