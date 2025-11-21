{{ config(materialized='view') }}
{% set moneyline_parameter_higher = 2000 %}
{% set moneyline_amount = -130 %}

-- betting strategy derived from ml/ml_moneyline_bins analysis, probably want to rerun that every month during the season to readjust.
-- need to add case when statements to identify these games during next season and highlight them on the schedule table.
with bet_games as (
    select *
    from {{ ref('ml_past_games_odds_analysis') }}
    where (
        (home_moneyline between '{{ moneyline_amount }}' and '{{ moneyline_parameter_higher }}' and home_team_predicted_win_pct >= 0.55)
        or (away_moneyline between '{{ moneyline_amount }}' and '{{ moneyline_parameter_higher }}' and away_team_predicted_win_pct >= 0.55)
        or (home_moneyline between 170 and '{{ moneyline_parameter_higher }}' and home_team_predicted_win_pct >= 0.50)
        or (away_moneyline between 170 and '{{ moneyline_parameter_higher }}' and away_team_predicted_win_pct >= 0.50)
    )
),

aggs as (
    select
        ml_accuracy,
        ml_prediction,
        count(*) as num_bets,
        round(avg(away_moneyline), 3) as avg_away_moneyline,
        round(avg(home_moneyline), 3) as avg_home_moneyline,
        round(avg(ml_money_col), 3) as avg_ml_money_col,
        sum(ml_money_col) as sum_ml_money_col
    from bet_games
    group by ml_accuracy, ml_prediction
)

select *
from aggs
