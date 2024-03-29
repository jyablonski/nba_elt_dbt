{% macro convert_team_names(column_name) %}

CASE WHEN {{ column_name }} = 'GS' THEN 'GSW'
     WHEN {{ column_name }} = 'LA' THEN 'LAL'
     WHEN {{ column_name }} = 'PHO' THEN 'PHX'
     WHEN {{ column_name }} = 'CHO' THEN 'CHA'
     WHEN {{ column_name }} = 'BRK' THEN 'BKN'
     WHEN {{ column_name }} = 'NY' THEN 'NYK'
     WHEN {{ column_name }} = 'SA' THEN 'SAS'
     WHEN {{ column_name }} = ' SA' THEN 'SAS'
     WHEN {{ column_name }} = 'NO' THEN 'NOP'
     ELSE {{ column_name }}
END
{% endmacro %}
