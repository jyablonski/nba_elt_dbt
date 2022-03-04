/* add in team ranks wins, avg rank of both teams for that game etc */

with new_odds as (
        select
        proper_date as date,
        day_name as day,
        proper_time,
        avg_team_rank,
        home_team,
        away_team,
        CONCAT(start_time, ' PM') as start_time,
        case when home_moneyline::numeric > 0 then concat('+', home_moneyline::text)
             else home_moneyline::text end as home_moneyline,
        case when away_moneyline::numeric > 0 then concat('+', away_moneyline::text)
             else away_moneyline::text end as away_moneyline
    from {{ ref('prep_schedule_table') }}
    where proper_date >= ((current_date)::date)
), 
aws_schedule_table as (

    select
        date,
        day,
        proper_time,
        avg_team_rank,
        start_time,
        home_team,
        away_team,
        case when home_moneyline is null then home_team
                  else CONCAT(home_team, ' (', home_moneyline, ')') end as home_team_odds,
        case when away_moneyline is null then away_team
                  else CONCAT(away_team, ' (', away_moneyline, ')') end as away_team_odds
    from new_odds
    where date >= ((current_date)::date)
)


select *
from aws_schedule_table
