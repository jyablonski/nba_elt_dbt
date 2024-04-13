{{ config(materialized='incremental') }}

with game_logs as (
    select 1
)

select *
from game_logs
