select *
from {{ ref('fact_boxscores') }}
    left join {{ source('nba_source', 'team_top_players') }}
        on fact_boxscores.team = team_top_players.team
where
    fact_boxscores.team != team_top_players.team
