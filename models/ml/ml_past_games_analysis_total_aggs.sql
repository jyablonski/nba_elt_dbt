{{ config(enabled = false) }}

-- this model grabs all ml predictions from entire season and runs aggregations to find correct prediction %.
-- it excludes tonight's games because we don't know whether the ml model is correct or incorrect on those predictions yet.

-- tonights_games_ml has to be a source bc im making that table externally in an ecs python script

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
    from {{ source('ml_models', 'tonights_games_ml') }}
    where game_date::date < date({{ dbt.current_timestamp() }} - interval '6 hour')
),

schedule_wins as (
    select
        a.team as home_team,
        s.game_date,
        s.outcome as outcome
    from {{ ref('prep_schedule_analysis') }} as s
        left join {{ ref('staging_seed_team_attributes') }} as a on s.team = a.team_acronym
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
        case
            when
                ml_prediction = actual_outcome then 1 else 0
        end as ml_accuracy
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
        'join' as join_col,
        count(*) as tot_games
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
