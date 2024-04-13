select *
from {{ ref('boxscores') }}
    left join {{ source('nba_source', 'top_team_players') }}
        on boxscores.team = top_team_players.team
where
    boxscores.team != top_team_players.team
