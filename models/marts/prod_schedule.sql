/* add in team ranks wins, avg rank of both teams for that game etc */
with aws_schedule_table as (

    select
        proper_date as date,
        day_name as day,
        proper_time,
        avg_team_rank,
        CONCAT(start_time, ' PM') as start_time,
        case when home_moneyline is null then home_team
                  else CONCAT(home_team, ' (', home_moneyline, ')') end as home_team,
        case when away_moneyline is null then away_team
                  else CONCAT(away_team, ' (', away_moneyline, ')') end as away_team
    from {{ ref('prep_schedule_table') }}
    where proper_date >= ((current_date)::date)
)


select *
from aws_schedule_table
