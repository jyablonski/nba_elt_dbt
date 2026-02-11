{{ config(enabled = false) }}

{% set moneyline_parameter_higher = 2000 %}
{% set moneyline_amount = -130 %}

with bet_games as (
    select *
    from {{ ref('ml_past_games_odds_analysis') }}
    where (
        (home_moneyline between '{{ moneyline_amount }}' and '{{ moneyline_parameter_higher }}' and home_team_predicted_win_pct >= 0.55)
        or (away_moneyline between '{{ moneyline_amount }}' and '{{ moneyline_parameter_higher }}' and away_team_predicted_win_pct >= 0.55)
        or (home_moneyline between 170 and '{{ moneyline_parameter_higher }}' and home_team_predicted_win_pct >= 0.50)
        or (away_moneyline between 170 and '{{ moneyline_parameter_higher }}' and away_team_predicted_win_pct >= 0.50)
    )
)

select *
from bet_games
