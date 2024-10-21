{% macro generate_season_type(column_name) %}  
    case when {{ column_name }} < '2025-04-15' then 'Regular Season'
         when {{ column_name }} >= '2025-04-15' and {{ column_name }} < '2025-04-20' then 'Play-In'
         else 'Playoffs' 
    END
{%- endmacro %}