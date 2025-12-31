{% macro select_test() %}

    {% set query %}

    SELECT 1::integer as test

    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}


{% endmacro %}
