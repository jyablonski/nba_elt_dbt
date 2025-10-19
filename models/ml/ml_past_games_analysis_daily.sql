{{ config(enabled = false) }}

-- this model grabs all ml predictions from entire season and runs aggregations to find correct prediction %.
-- it excludes tonight's games because we don't know whether the ml model is correct or incorrect on those predictions yet.
-- ml_game_predictions has to be a source bc im making that table externally in an ecs python script

{% set rolling_avg_parameter = 6 %}

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
        prep_schedule_analysis.game_date as game_date,
        prep_schedule_analysis.outcome as outcome
    from {{ ref('prep_schedule_analysis') }} as prep_schedule_analysis
        left join {{ ref('dim_teams') }} as dim_teams 
            on dim_teams.team_acronym = prep_schedule_analysis.team
    where prep_schedule_analysis.location = 'H'
),

final as (
    select
        *,
        case when
            home_team_predicted_win_pct >= 0.5 then 'Home Win'
            else 'Road Win' end as ml_prediction,
        case when outcome = 'W' then 'Home Win' else 'Road Win' end as actual_outcome
    from my_cte
    left join schedule_wins using (home_team, game_date)
),

-- the data points actually broken down
-- ml is correct when ml_accuracy = 1
game_predictions as (
    select distinct *,
        case when ml_prediction = actual_outcome then 1 else 0 end as ml_accuracy
    from final
),

-- reminder - pivot is fucked on postgres so i just split every group metric into a separate cte.
final_aggs_correct as (
    select
        ml_accuracy,
        game_date,
        count(*) as tot_correct_predictions
    from game_predictions
    where ml_accuracy = 1
    group by
        ml_accuracy,
        game_date
),

final_aggs_incorrect as (
    select
        ml_accuracy,
        game_date,
        count(*) as tot_incorrect_predictions
    from game_predictions
    where ml_accuracy = 0
    group by
        ml_accuracy,
        game_date
),

final_aggs_sum as (
    select
        count(*) as tot_games,
        game_date
    from game_predictions
    group by game_date
),

final_aggs_tot as (
    select
        final_aggs_sum.game_date,
        coalesce(final_aggs_correct.tot_correct_predictions, 0) as tot_correct_predictions,
        coalesce(final_aggs_incorrect.tot_incorrect_predictions, 0) as tot_incorrect_predictions,
        final_aggs_sum.tot_games,
        round((coalesce(final_aggs_correct.tot_correct_predictions, 0)::numeric) / (final_aggs_sum.tot_games::numeric), 3)::numeric as ml_prediction_pct
    from final_aggs_sum
    left join final_aggs_correct using (game_date)
    left join final_aggs_incorrect using (game_date)
    order by final_aggs_sum.game_date desc
),

rolling_avg as (
    select
        *,
        round(avg(ml_prediction_pct) over (order by game_date ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 3)::numeric as ml_prediction_pct_ma
    from final_aggs_tot
    order by game_date desc
)

select *
from rolling_avg