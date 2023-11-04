{{ config(materialized='view') }}

select
    game_date,
    home_team,
    home_moneyline as home_team_odds,
    home_team_predicted_win_pct,
    away_team,
    away_moneyline as away_team_odds,
    away_team_predicted_win_pct
from {{ source('ml_models', 'tonights_games_ml') }}
where game_date = current_date
