{{ config(materialized='view') }}

select *
from {{ ref('schedule') }}
where game_date > current_date
order by game_date
