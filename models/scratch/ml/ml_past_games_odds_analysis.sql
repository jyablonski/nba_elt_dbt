{{ config(materialized='view', enabled = false) }}
/*
this model grabs all ml predictions from entire season and joins the odds data

end goal is to do some odds + ML analysis and see if i can find value in automating when bets should be placed for maximum $ returns.

right now this is just the base table, the actual analysis is gnna have to do some joins with ml_accuracy, home+away moneyline, and actual outcome
*/
{% set bet_parameter = 10 %}


with my_cte as (
    select
        home_team,
        away_team,
        game_date::date as game_date,
        home_team_rank,
        home_days_rest,
        home_team_avg_pts_scored,
        home_team_avg_pts_scored_opp,
        home_team_win_pct,
        home_team_win_pct_last10,
        home_is_top_players,
        away_team_rank,
        away_days_rest,
        away_team_avg_pts_scored,
        away_team_avg_pts_scored_opp,
        away_team_win_pct,
        away_team_win_pct_last10,
        away_is_top_players,
        home_team_predicted_win_pct,
        away_team_predicted_win_pct
    from {{ source('ml', 'ml_game_predictions') }}
    where
        game_date::date < date({{ dbt.current_timestamp() }} - interval '6 hour')
        and game_date::date >= '{{ var("prediction_start_date") }}'
),

schedule_wins as (
    select
        a.team as home_team,
        s.game_date,
        s.outcome
    from {{ ref('int_schedule_analysis') }} as s
        left join {{ ref('dim_teams') }} as a on s.team = a.team_acronym
    where location = 'H'
),

final as (
    select
        *,
        case
            when home_team_predicted_win_pct >= 0.5 then 'Home Win'
            else 'Road Win'
        end as ml_prediction,
        case when outcome = 'W' then 'Home Win' else 'Road Win' end as actual_outcome
    from my_cte
        left join schedule_wins using (home_team, game_date)
),

-- the data points actually broken down
-- ml is correct when ml_accuracy = 1
game_predictions as (
    select distinct
        *,
        case when ml_prediction = actual_outcome then 1 else 0 end as ml_accuracy
    from final
),

home_odds as (
    select
        a.team as home_team,
        date as game_date,
        moneyline as home_moneyline
    from {{ ref('fact_odds_data') }}
        left join {{ ref('dim_teams') }} as a using (team_acronym)
),

away_odds as (
    select
        a.team as away_team,
        date as game_date,
        moneyline as away_moneyline
    from {{ ref('fact_odds_data') }}
        left join {{ ref('dim_teams') }} as a using (team_acronym)
),

final_table as (
    select
        *,                                  -- your round('{{ bet_parameter }}' + (the original bet * money multiplier)
        case
            when ml_accuracy = 1 and ml_prediction = 'Home Win' and home_moneyline < 0
                then round('{{ bet_parameter }}' * (-100 / home_moneyline), 2)
            when ml_accuracy = 1 and ml_prediction = 'Home Win' and home_moneyline > 0
                then round('{{ bet_parameter }}' * (home_moneyline / 100), 2)
            when ml_accuracy = 1 and ml_prediction = 'Road Win' and away_moneyline < 0
                then round('{{ bet_parameter }}' * (-100 / away_moneyline), 2)
            when ml_accuracy = 1 and ml_prediction = 'Road Win' and away_moneyline > 0
                then round('{{ bet_parameter }}' * (away_moneyline / 100), 2)
            when ml_accuracy = 0 then -10
            else -10000  -- im testing to make sure it never hits -10000 - if it does then there's an error
        end as ml_money_col,
        case
            when home_moneyline > 0 then round(100 / (home_moneyline + 100), 3)
            else round(abs(home_moneyline) / (abs(home_moneyline) + 100), 3)
        end as home_implied_probability,
        case
            when away_moneyline > 0 then round(100 / (away_moneyline + 100), 3)
            else round(abs(away_moneyline) / (abs(away_moneyline) + 100), 3)
        end as away_implied_probability
    from game_predictions
        left join home_odds using (home_team, game_date)
        left join away_odds using (away_team, game_date)
    order by game_date desc
)

-- predictions are 8 correct, 7 incorrect
-- 5 predicted road wins, 10 predicted home wins
-- 4 actual road wins, 11 home wins

select *
from final_table
-- from game_predictions
