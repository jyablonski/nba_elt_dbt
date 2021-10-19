with recent_games as (
    select
        team,
        opponent,
        date,
        outcome,
        pts_scored,
        pts_scored_opp,
        mov,
        team_max_score,
        team_avg_score,
        pts_color,
        team_logo
    from {{ ref('prep_recent_games_teams')}}

),

recent_date as (
    select max(date) as date
 from {{ ref('staging_aws_boxscores_table')}}
),

team_pts_scored as (
    select
        *
    from recent_games
    inner join recent_date using (date)
)

select *
from team_pts_scored
