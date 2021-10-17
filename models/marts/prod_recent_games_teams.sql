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

)

select *
from recent_games