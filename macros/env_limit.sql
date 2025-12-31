-- Macro used to apply limits to tables + views in nonprod environments

{% macro env_limit(sample_size=1000) -%}
    {%- if target.name == 'prod' -%}

    --if running in prod environment, do nothing

    {%- else -%}

    -- if running in non-prod env, limit data by the sample size
    LIMIT {{ sample_size }}

    {%- endif -%}
{%- endmacro %}
