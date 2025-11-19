with leads as (
    select distinct
        -- team is basically the "winner"
        home_team,
        case
            when home_team = winning_team then home_team
            when away_team = winning_team then away_team
            else 'HELP'
        end as team,
        case
            when home_team = winning_team then away_team
            when home_team != winning_team then home_team
            else 'HELP'
        end as opponent,
        case
            when home_team = winning_team then abs(max_home_lead)
            when away_team = winning_team then abs(max_away_lead)
            else '999'
        end as max_team_lead,
        case
            when home_team = winning_team then abs(max_away_lead)
            when home_team != winning_team then abs(max_home_lead)
            else '999'
        end as max_opponent_lead
    from {{ ref('pbp') }}
),

team_pts_scored as (
    select
        team,
        opponent,
        game_date,
        outcome,
        pts_scored,
        pts_scored_opp,
        mov,
        max_team_lead,
        max_opponent_lead,
        team_max_score,
        team_avg_score,
        pts_color::text as pts_color,
        opp_pts_color::text as opp_pts_color,
        team_logo,
        opp_logo,
        home_team,
        case
            when team = home_team then 'Vs.'
            else '@'
        end as new_loc
    from {{ ref('int_recent_games_teams') }}
        inner join {{ ref('most_recent_game') }} on int_recent_games_teams.game_date = most_recent_game.max_game_date
        left join leads using (team, opponent)
    where outcome = 'W'
)

select *
from team_pts_scored
