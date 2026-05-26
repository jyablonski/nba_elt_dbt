{{ config(materialized='view') }}

select
    *,
    {{ dbt.current_timestamp() }} as __created_at
from {{ ref('schedule') }}
where game_date > current_date
order by game_date
