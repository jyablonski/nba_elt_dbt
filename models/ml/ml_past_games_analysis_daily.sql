/*
this model grabs all ml predictions from entire season and runs aggregations to find correct prediction %.
it excludes tonight's games because we don't know whether the ml model is correct or incorrect on those predictions yet.

tonights_games_ml has to be a source bc im making that table externally in an ecs python script
*/
{% set rolling_avg_parameter = 6 %}
-- keep in mind for a 7-day rolling average you have to set the rolling_average_parameter to 6 (n -1)

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

-- reminder - pivot is fucked on postgres so i just split every group metric into a separate cte.
final_aggs_correct as (
    select
        ml_accuracy,
        proper_date,
        count(*) as tot_correct_predictions
    from game_predictions
    where ml_accuracy = 1
    group by 1, 2
),

final_aggs_incorrect as (
    select
        ml_accuracy,
        proper_date,
        count(*) as tot_incorrect_predictions
    from game_predictions
    where ml_accuracy = 0
    group by 1, 2
),

final_aggs_sum as (
    select
        count(*) as tot_games,
        proper_date
    from game_predictions
    group by 2
),

final_aggs_tot as (
    select 
        s.proper_date,
        coalesce(c.tot_correct_predictions, 0) as tot_correct_predictions,
        coalesce(i.tot_incorrect_predictions, 0) as tot_incorrect_predictions,
        s.tot_games,
        round((coalesce(c.tot_correct_predictions, 0)::numeric) / (tot_games::numeric), 3)::numeric as ml_prediction_pct
    from final_aggs_sum s
    left join final_aggs_correct c using (proper_date)
    left join final_aggs_incorrect i using (proper_date)
    order by proper_date desc
),

rolling_avg as (
    select
        *,
        round(avg(ml_prediction_pct) over(order by proper_date ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 3)::numeric as ml_prediction_pct_ma
    from final_aggs_tot
    order by proper_date desc
)

select *
from rolling_avg
