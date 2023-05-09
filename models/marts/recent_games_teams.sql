/* only grabbing latest games, the prep table has everything for qa purposes */
with recent_date as (
    select
        max(date) as date
    from {{ ref('staging_aws_boxscores_incremental_table')}}
),

leads as (
    select distinct
        -- team is basically the "winner"
        case when home_team = winning_team then home_team
             when away_team = winning_team then away_team
             else 'HELP'
             end as team,
        case when home_team = winning_team then away_team
             when home_team != winning_team then home_team
             else 'HELP'
             end as opponent,
        case when home_team = winning_team then abs(max_home_lead)
             when away_team = winning_team then abs(max_away_lead)
             else '999' end as max_team_lead,
        case when home_team = winning_team then abs(max_away_lead)
             when home_team != winning_team then abs(max_home_lead)
             else '999'
             end as max_opponent_lead,
             home_team
    from {{ ref('prod_pbp') }}
),

team_pts_scored as (
    select 
        team,
        opponent,
        date,
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
        case when team = home_team then 'Vs.'
             else '@' end as new_loc
    from {{ ref('prep_recent_games_teams')}}
    inner join recent_date using (date)
    left join leads using (team, opponent)
    where outcome = 'W'
)

select *
from team_pts_scored