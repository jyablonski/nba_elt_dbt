{{ config(materialized='view') }}

-- make it a view bc ml pipeline has to run after dbt

-- the calculations for the great value case whens are inspired from the betting strategy +
-- moneyline bin analysis views
select
    schedule.home_team,
    schedule.away_team,
    avg_team_rank,
    home_team_odds,
    away_team_odds,
    start_time,
    schedule.game_date,
    home_moneyline,
    away_moneyline,
    home_team_predicted_win_pct,
    away_team_predicted_win_pct,
    -- booleans used in the dashboard cells to highlight teams w/ good bet value
    {{ is_great_bet_value('home_moneyline', 'home_team_predicted_win_pct') }} as home_is_great_value,
    {{ is_great_bet_value('away_moneyline', 'away_team_predicted_win_pct') }} as away_is_great_value
from {{ ref('schedule') }}
    inner join {{ source('ml', 'ml_game_predictions') }}
        on
            schedule.home_team = ml_game_predictions.home_team
            and schedule.away_team = ml_game_predictions.away_team
            and schedule.game_date = ml_game_predictions.game_date
where schedule.game_date = current_date
order by
    schedule.game_ts
