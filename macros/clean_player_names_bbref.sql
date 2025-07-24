{% macro clean_player_names_bbref(column_name) %}

CASE WHEN {{ column_name }} = 'Mo Bamba' THEN 'Mohamed Bamba'
     WHEN {{ column_name }} = 'Herb Jones' THEN 'Herbert Jones'
     WHEN {{ column_name }} = 'Nicolas Claxton' THEN 'Nic Claxton'
     WHEN {{ column_name }} = 'Enes Kanter' THEN 'Enes Freedom'
     WHEN {{ column_name }} = 'Cameron Thomas' THEN 'Cam Thomas'
     WHEN {{ column_name }} = 'Juan Hernangomez' THEN 'Juancho Hernangomez'
     WHEN {{ column_name }} = 'Didi Louzada' THEN 'Marcos Louzada Silva'
     WHEN {{ column_name }} = 'Wesley Iwundu' THEN 'Wes Iwundu'
     WHEN {{ column_name }} = 'Scotty Pippen ' THEN 'Scotty Pippen'
     ELSE {{ column_name }}
END
{% endmacro %}
