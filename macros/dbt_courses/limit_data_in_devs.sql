{% macro limit_data_in_devs(column_name, dev_days_of_data=3) %}
    -- where {{ column_name }} >= dateadd('day', - {{ dev_days_of_data }}, current_timestamp)
    where timestamp({{ column_name }}) >= date_add(current_timestamp, INTERVAL -{{ dev_days_of_data }} day)
{% endmacro %}