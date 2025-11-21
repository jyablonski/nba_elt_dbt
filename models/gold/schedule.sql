/* add in team ranks wins, avg rank of both teams for that game etc */

with new_odds as (
    select
        game_date,
        day_name,
        game_ts,
        avg_team_rank,
        home_team,
        away_team,
        home_moneyline as home_moneyline_raw,
        away_moneyline as away_moneyline_raw,
        concat(start_time, ' PM') as start_time,
        case
            when home_moneyline::numeric > 0 then concat('+', home_moneyline::text)
            else home_moneyline::text
        end as home_moneyline,
        case
            when away_moneyline::numeric > 0 then concat('+', away_moneyline::text)
            else away_moneyline::text
        end as away_moneyline
    from {{ ref('int_schedule_table') }}
),


team_logo_home as (
    select
        team as home_team,
        team_logo as home_team_logo
    from {{ ref('dim_teams') }}
),

team_logo_away as (
    select
        team as away_team,
        team_logo as away_team_logo
    from {{ ref('dim_teams') }}
),



aws_schedule_table as (

    select
        game_date,
        day_name,
        game_ts,
        avg_team_rank,
        start_time,
        home_team,
        away_team,
        home_moneyline_raw,
        away_moneyline_raw,
        home_team_logo,
        away_team_logo,
        case
            when home_moneyline is null then home_team
            else concat(home_team, ' (', home_moneyline, ')')
        end as home_team_odds,
        case
            when away_moneyline is null then away_team
            else concat(away_team, ' (', away_moneyline, ')')
        end as away_team_odds
    from new_odds
        left join team_logo_home using (home_team)
        left join team_logo_away using (away_team)
    order by game_ts

)


select *
from aws_schedule_table
