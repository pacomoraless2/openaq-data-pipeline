{% test is_not_future_year(model, column_name) %}

    select *
    from {{ model }}
    -- Filtramos las filas donde el año sea mayor que el año actual del sistema
    where {{ column_name }} > EXTRACT(YEAR FROM CURRENT_DATE())

{% endtest %}