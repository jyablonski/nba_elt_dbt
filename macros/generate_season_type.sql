{% macro generate_season_type(column_name) %}  
    case when {{ column_name }} < '2024-04-11' then 'Regular Season'
         when {{ column_name }} >= '2024-04-11' and {{ column_name }} < '2024-04-15' then 'Play-In'
         else 'Playoffs' 
    END
{%- endmacro %}