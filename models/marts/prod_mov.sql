/* TODO - ADD STANDINGS (ex. 24-0) */

with mov_table as (
    select
        team,
        full_team,
        game_id,
        date,
        outcome,
        opponent,
        pts_scored,
        pts_scored_opp,
        mov

    from {{ ref('prep_recent_games_teams')}}
)

select *
from mov_table
