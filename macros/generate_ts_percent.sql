{% macro generate_ts_percent(pts, fga, fta) -%}

CASE WHEN {{pts}} = 0 AND {{fga}} = 0 AND {{fta}} = 0 THEN NULL
     ELSE ({{ pts }} / (2 * ({{ fga }} + ({{ fta }} * 0.44))))
END
{%- endmacro %}