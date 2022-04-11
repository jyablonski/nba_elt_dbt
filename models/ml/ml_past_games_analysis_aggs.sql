/*
this model grabs all ml predictions from entire season and runs aggregations to find correct prediction %.
it excludes tonight's games because we don't know whether the ml model is correct or incorrect on those predictions yet.

tonights_games_ml has to be a source bc im making that table externally in an ecs python script
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

final_aggs as (
    select
        ml_accuracy,
        count(*) as tot_correct_predictions,
        'join' as join_col
    from game_predictions
    group by 1, 3
),

final_aggs_sum as (
    select
        count(*) as tot_games,
        'join' as join_col
    from game_predictions
),

final_aggs_tot as (
    select 
        *,
        round((tot_correct_predictions::numeric) / (tot_games::numeric), 3)::numeric as ml_prediction_pct
    from final_aggs
    left join final_aggs_sum using (join_col)
    where ml_accuracy = 1
),


-- predictions are 8 correct, 7 incorrect
-- 5 predicted road wins, 10 predicted home wins
-- 4 actual road wins, 11 home wins

final_summary as (
    select 
        tot_correct_predictions,
        tot_games,
        ml_prediction_pct
    from final_aggs_tot
)

select *
--from game_predictions
from final_summary