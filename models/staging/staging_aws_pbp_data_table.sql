/* 12-13-21 - some issue where 2 games have had quarter be null for 1st quarter ?? look into this */
{{ config(materialized='incremental') }}

with pbp_cte as (
    select
        descriptionplayvisitor::text as description_play_visitor,
        awayscore::text as away_score,
        score::text as score,
        homescore::text as home_score,
        descriptionplayhome::text as description_play_home,
        coalesce(numberperiod::text, '1st Quarter')::text as quarter,
        hometeam::text as home_team,
        awayteam::text as away_team,
        scoreaway::numeric as score_away,
        scorehome::numeric as score_home,
        marginscore::numeric as margin_score,
        date::date as date,
        substr(timequarter, 1, length(timequarter) - 2)::text as time_quarter,
        case when date < '2022-04-11' then 'Regular Season'
             when date >= '2022-04-11' and date < '2022-04-16' then 'Play-In'
             else 'Playoffs' 
            end as season_type
    from {{ source('nba_source', 'aws_pbp_data_source')}}
)

select *
from pbp_cte

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- only grab records where date is greater than the max date of the existing records in the tablegm
  where date > (select max(date) from {{ this }})

{% endif %}