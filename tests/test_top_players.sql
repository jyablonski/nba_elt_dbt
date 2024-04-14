select *
from {{ ref('boxscores') }}
    left join {{ source('nba_source', 'team_top_players') }}
        on boxscores.team = team_top_players.team
where
    boxscores.team != team_top_players.team
