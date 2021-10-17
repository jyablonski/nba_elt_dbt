with standings as (
    select
        team,
        team_full,
        conference,
        wins,
        losses,
        games_played,
        active_injuries
    from {{ ref('prep_standings_table')}}

)

select *
from standings