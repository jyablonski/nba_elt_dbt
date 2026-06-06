/* add in team ranks wins, avg rank of both teams for that game etc */

with new_odds as (
    select
        schedule_table.game_date,
        schedule_table.day_name,
        schedule_table.game_ts,
        schedule_table.avg_team_rank,
        schedule_table.home_team,
        schedule_table.away_team,
        schedule_table.home_moneyline as home_moneyline_raw,
        schedule_table.away_moneyline as away_moneyline_raw,
        concat(schedule_table.start_time, ' PM') as start_time,
        series_games.round_name as series_round,
        series_games.series_status_before_game as series_status,
        series_games.series_game_number,
        case
            when schedule_table.home_moneyline::numeric > 0 then concat('+', schedule_table.home_moneyline::text)
            else schedule_table.home_moneyline::text
        end as home_moneyline,
        case
            when schedule_table.away_moneyline::numeric > 0 then concat('+', schedule_table.away_moneyline::text)
            else schedule_table.away_moneyline::text
        end as away_moneyline
    from {{ ref('int_schedule_table') }} as schedule_table
        left join {{ ref('int_playoff_series_games') }} as series_games
            on schedule_table.game_date = series_games.game_date
            and least(schedule_table.home_team_acronym, schedule_table.away_team_acronym) = series_games.team_a
            and greatest(schedule_table.home_team_acronym, schedule_table.away_team_acronym) = series_games.team_b
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
        series_round,
        series_status,
        series_game_number,
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
