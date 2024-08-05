{% macro tipo_tienda(column_name) %}
    case
    when {{ column_name }} < 50 then 'Pequeña'
    when {{ column_name }} >= 50 and {{ column_name }} < 75 then 'Mediana'
    when {{ column_name }} >= 75 then 'Grande'
    else 'Desconocido'
    end
{% endmacro %}