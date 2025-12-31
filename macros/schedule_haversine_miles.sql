-- macro used to calculate team schedule travel distance
{% macro schedule_haversine_miles(lat1, lon1, lat2, lon2) %}
    3959 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS({{ lat2 }} - {{ lat1 }}) / 2), 2) +
        COS(RADIANS({{ lat1 }})) * COS(RADIANS({{ lat2 }})) *
        POWER(SIN(RADIANS({{ lon2 }} - {{ lon1 }}) / 2), 2)
    ))
{% endmacro %}
