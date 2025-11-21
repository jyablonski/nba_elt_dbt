{{ config(enabled = false) }}

-- this model grabs all ml predictions from entire season and runs aggregations to find correct prediction %.
-- it excludes tonight's games because we don't know whether the ml model is correct or incorrect on those predictions yet.

-- ml_game_predictions has to be a source bc im making that table externally in an ecs python script

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
    where game_date::date < date({{ dbt.current_timestamp() }} - interval '6 hour')
),

schedule_wins as (
    select
        dim_teams.team as home_team,
        prep_schedule_analysis.game_date,
        prep_schedule_analysis.outcome as outcome
    from {{ ref('int_schedule_analysis') }} as prep_schedule_analysis
        left join {{ ref('dim_teams') }} as dim_teams 
            on prep_schedule_analysis.team = dim_teams.team_acronym
    where prep_schedule_analysis.location = 'H'
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
        case
            when ml_prediction = actual_outcome then 1 
            else 0
        end as ml_accuracy
    from final
),

final_aggs as (
    select
        ml_accuracy,
        count(*) as tot_correct_predictions
    from game_predictions
    group by ml_accuracy
),

final_aggs_sum as (
    select
        count(*) as tot_games
    from game_predictions
),

final_aggs_tot as (
    select
        final_aggs.ml_accuracy,
        final_aggs.tot_correct_predictions,
        final_aggs_sum.tot_games,
        round((final_aggs.tot_correct_predictions::numeric) / (final_aggs_sum.tot_games::numeric), 3)::numeric as ml_prediction_pct
    from final_aggs
        cross join final_aggs_sum
    where final_aggs.ml_accuracy = 1
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