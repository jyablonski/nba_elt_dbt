{% macro ingestion_activity_on_check_date(column_name) %}
(
    {{ column_name }}::timestamp >= ({{ dbt.current_timestamp() }} at time zone '{{ var("dbt_date:time_zone") }}')::date
    and {{ column_name }}::timestamp < ({{ dbt.current_timestamp() }} at time zone '{{ var("dbt_date:time_zone") }}')::date + interval '1 day'
)
{% endmacro %}

{% macro bronze_ingestion_check(bronze_table) %}
select
    '{{ bronze_table }}' as bronze_source_table,
    count(*) as records_today,
    max(greatest(created_at::timestamp, modified_at::timestamp)) as latest_activity_at
from {{ source('bronze', bronze_table) }}
where
    {{ ingestion_activity_on_check_date('created_at') }}
    or {{ ingestion_activity_on_check_date('modified_at') }}
{% endmacro %}
