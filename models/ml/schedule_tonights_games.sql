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
    case
        when
            ((home_moneyline >= -130 or home_moneyline >= 200) and home_team_predicted_win_pct >= 0.55)
            or (home_moneyline >= 170 and home_team_predicted_win_pct >= 0.50) then 1
        else 0
    end as home_is_great_value,
    case
        when
            ((away_moneyline >= -130 or away_moneyline >= 200) and away_team_predicted_win_pct >= 0.55)
            or (away_moneyline >= 170 and away_team_predicted_win_pct >= 0.50) then 1
        else 0
    end as away_is_great_value
from {{ ref('schedule') }}
    inner join {{ source('ml_models', 'tonights_games_ml') }}
        on
            schedule.home_team = tonights_games_ml.home_team
            and schedule.away_team = tonights_games_ml.away_team
            and schedule.game_date = tonights_games_ml.game_date
where schedule.game_date = current_date
