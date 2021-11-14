{% macro generate_ord_numbers(column_name) %}

CASE WHEN {{ column_name }} in (11, 12, 13) THEN concat({{ column_name }}, 'th')
     WHEN {{ column_name }} % 10 = 1 THEN concat({{ column_name }}, 'st')
     WHEN {{ column_name }} % 10 = 2 THEN concat({{ column_name }}, 'nd')
     WHEN {{ column_name }} % 10 = 3 THEN concat({{ column_name }}, 'rd')
     ELSE concat( {{ column_name }}, 'th')
END
{% endmacro %}
