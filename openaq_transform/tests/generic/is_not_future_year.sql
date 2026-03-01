{% test is_not_future_year(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} > EXTRACT(YEAR FROM CURRENT_DATE())

{% endtest %}