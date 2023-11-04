with home_teams as (
    select distinct
        game_date,
        season_type,
        game_id,
        home_team as opp,
        home_team_full as opp_full,
        away_team as team,
        away_team_full as team_full,
        'Road' as location,
        winning_team,
        losing_team,
        max_home_lead,
        max_away_lead,
        case
            when winning_team = home_team then 'Home'
            else 'Road'
        end as winning_team_loc
    from {{ ref('prep_pbp_table') }}
    order by game_date
),

road_teams as (
    select distinct
        game_date,
        season_type,
        game_id,
        away_team as opp,
        away_team_full as opp_full,
        home_team as team,
        home_team_full as team_full,
        'Home' as location,
        winning_team,
        losing_team,
        max_home_lead,
        max_away_lead,
        case
            when winning_team = home_team then 'Home'
            else 'Road'
        end as winning_team_loc
    from {{ ref('prep_pbp_table') }}
    order by game_date
),

combo as (
    select *
    from road_teams
    union
    select *
    from home_teams
    order by game_date, game_id
),

full_table as (
    select
        *,
        case
            when location = 'Home' then abs(max_home_lead)
            else abs(max_away_lead)
        end as max_team_lead,
        case
            when location = 'Home' then abs(max_away_lead)
            else abs(max_home_lead)
        end as max_opp_lead,
        case
            when team = winning_team then 'W'
            else 'L'
        end as outcome
    from combo
)

select *
from full_table
