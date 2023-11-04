/* TODO - ADD STANDINGS (ex. 24-0) */

with mov_table as (
    select
        team,
        full_team,
        game_id,
        game_date,
        outcome,
        opponent,
        pts_scored,
        pts_scored_opp,
        mov

    from {{ ref('prep_recent_games_teams') }}
),

record as (
    select distinct
        team,
        record
    from {{ ref('prep_past_schedule_analysis') }}
)

select *
from mov_table
    left join record using (team)
