{% macro generate_season_type(column_name) %}
    case
        when {{ column_name }} < (
            select min(start_date) from {{ source('bronze', 'play_in_details') }}
        ) then 'Regular Season'
        when {{ column_name }} between (
            select min(start_date) from {{ source('bronze', 'play_in_details') }}
        ) and (
            select max(end_date) from {{ source('bronze', 'play_in_details') }}
        ) then 'Play-In'
        else 'Playoffs'
    end
{% endmacro %}