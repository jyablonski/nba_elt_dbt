{{ config(materialized='view') }}

select
    game_date,
    day_name,
    game_ts,
    avg_team_rank,
    start_time,
    home_team,
    away_team,
    home_moneyline_raw,
    away_moneyline_raw,
    home_team_logo,
    away_team_logo,
    series_round,
    series_game_number,
    home_team_odds,
    away_team_odds,
    {{ dbt.current_timestamp() }} as __created_at
from {{ ref('schedule') }}
where game_date > current_date
order by game_date
