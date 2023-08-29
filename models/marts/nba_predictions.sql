{{ config(materialized='view') }}

select
    proper_date,
    home_team,
    home_moneyline as home_team_odds,
    home_team_predicted_win_pct,
    away_team,
    away_moneyline as away_team_odds,
    away_team_predicted_win_pct
from {{ source('ml_models', 'tonights_games_ml') }}
where proper_date = current_date
