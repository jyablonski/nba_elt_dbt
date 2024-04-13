select *
from {{ ref('boxscores') }}
    left join {{ source('nba_source', 'top_team_players') }}
        on boxscores.team = staging_seed_top_players.team
where
    boxscores.team != staging_seed_top_players.team
