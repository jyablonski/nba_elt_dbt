/*
this model grabs all ml predictions from entire season and joins the odds data

end goal is to do some odds + ML analysis and see if i can find value in automating when bets should be placed for maximum $ returns.

right now this is just the base table, the actual analysis is gnna have to do some joins with ml_accuracy, home+away moneyline, and actual outcome
*/

with my_cte as (
    select *
    from {{ source('ml_models', 'tonights_games_ml') }}
    where proper_date < date({{ dbt_utils.current_timestamp() }} - INTERVAL '6 hour')
),

schedule_wins as (
    select 
        a.team as home_team,
        s.date as proper_date,
        s.outcome as outcome
    from {{ ref('prep_schedule_analysis') }} as s
    left join {{ ref('staging_seed_team_attributes') }} as a on a.team_acronym = s.team
    where location = 'H'
),

final as (
    select 
        *,
        case when home_team_predicted_win_pct >= 0.5 then 'Home Win'
            else 'Road Win' end as ml_prediction,
        case when outcome = 'W' then 'Home Win' else 'Road Win' end as actual_outcome
    from my_cte
    left join schedule_wins using (home_team, proper_date)
),

-- the data points actually broken down
-- ml is correct when ml_accuracy = 1
game_predictions as (
    select distinct *,
        case when ml_prediction = actual_outcome then 1 else 0 end as ml_accuracy
    from final
),

home_odds as (
    select
        a.team as home_team,
        date as proper_date,
        moneyline as home_moneyline
    from {{ ref('staging_aws_odds_table') }}
    left join {{ ref('staging_seed_team_attributes') }} a using (team_acronym)
),

away_odds as (
    select
        a.team as away_team,
        date as proper_date,
        moneyline as away_moneyline
    from {{ ref('staging_aws_odds_table') }}
    left join {{ ref('staging_seed_team_attributes') }} a using (team_acronym)
),

final_table as (
    select
        *
    from game_predictions
    left join home_odds using (home_team, proper_date)
    left join away_odds using (away_team, proper_date)
)

-- predictions are 8 correct, 7 incorrect
-- 5 predicted road wins, 10 predicted home wins
-- 4 actual road wins, 11 home wins

select *
from final_table
-- from game_predictions