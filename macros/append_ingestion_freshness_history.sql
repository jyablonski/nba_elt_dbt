{% macro append_ingestion_freshness_history() %}
-- Replace placeholder history rows (null/zero records_today) for this check_date.
-- Skip insert when a row with legitimate data (records_today > 0) already exists.
delete from {{ this.schema }}.ingestion_freshness_history as history
using {{ this }} as current_run
where
    history.flag = current_run.flag
    and history.check_date = current_run.check_date
    and coalesce(history.records_today, 0) = 0;

insert into {{ this.schema }}.ingestion_freshness_history (
    feature_flag_id,
    flag,
    bronze_source_table,
    write_method,
    check_date,
    checked_at,
    records_today,
    latest_activity_at,
    freshness_status,
    is_fresh,
    history_inserted_at
)
select
    current_run.feature_flag_id,
    current_run.flag,
    current_run.bronze_source_table,
    current_run.write_method,
    current_run.check_date,
    current_run.checked_at,
    current_run.records_today,
    current_run.latest_activity_at,
    current_run.freshness_status,
    current_run.is_fresh,
    current_timestamp as history_inserted_at
from {{ this }} as current_run
where not exists (
    select 1
    from {{ this.schema }}.ingestion_freshness_history as history
    where
        history.flag = current_run.flag
        and history.check_date = current_run.check_date
        and coalesce(history.records_today, 0) > 0
);
{% endmacro %}
