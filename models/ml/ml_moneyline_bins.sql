{{ config(enabled = false) }}
{% set moneyline_parameter_higher = 2000 %}
{% set moneyline_amounts = range(-200, 200, 10) %}


-- This query bins moneyline_amounts in groups of 5 and creates a matrix to analyze the best profit margins to be betting on and find the sweet spot
-- It assumes the ML Model predicted a >= 55% win percentage for either team.
-- It runs a union all command for every bin (like 40 of them) and then aggregates profit up based on the bin
-- the idea behind setting the parameter at 2000 is i'm establishing the WORST moneyline odds id be willing to bet on for that specfic bin,
-- and then slowly incrementing it up to cover the entire range of -200 to 200.
-- 2022-06-18 - it currently is at $0.195 profit per $1 bet across 40 total games of > 55% predicted win % and moneyline between -130 and 2000
-- also bet on any team that have +170 odds or better w/ >= 50% predicted win %

with bets as (
{% for moneyline_amount in moneyline_amounts %}
    (
    select
        game_date,
        home_team,
        home_moneyline,
        home_team_predicted_win_pct,
        away_team,
        away_moneyline,
        away_team_predicted_win_pct,
        ml_prediction,
        actual_outcome,
        ml_money_col,
        case when ml_money_col > 0 then ml_money_col - 10
            else ml_money_col end as ml_money_col2,
        '{{ moneyline_amount }}' as moneyline_parameter_lower
    from {{ ref('ml_past_games_odds_analysis') }}
    where (
        (home_moneyline between '{{ moneyline_amount }}' and '{{ moneyline_parameter_higher }}'
            and home_team_predicted_win_pct >= 0.55)
        or (away_moneyline between '{{ moneyline_amount }}' and '{{ moneyline_parameter_higher }}'
            and away_team_predicted_win_pct >= 0.55)
        )
    )
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}

),

bet_aggs as (
    select
        sum(ml_money_col2) / 10 as profit,
        count(*) as num_bets,
        round((sum(ml_money_col2) / 10) / count(*), 3)::numeric as profit_per_bet,
        moneyline_parameter_lower,
        2000 as moneyline_parameter_higher
    from bets
    where
        (moneyline_parameter_lower::numeric not between 0 and 95)
        and (moneyline_parameter_lower::numeric not between -95 and 0)
        and (moneyline_parameter_lower::numeric != -100)
    group by moneyline_parameter_lower
    order by moneyline_parameter_lower::numeric desc
),

bet_aggs_lag as (
    select
        *,
        lag(profit) over (order by moneyline_parameter_lower::numeric desc) as prev_profit,
        profit - lag(profit) over (order by moneyline_parameter_lower::numeric desc) as profit_change,
        round(profit / lag(profit) over (order by moneyline_parameter_lower::numeric desc), 3)::numeric as profit_pct_change,
        {{ dbt.current_timestamp() }} as current_date
    from bet_aggs

)

select *
from bet_aggs_lag
