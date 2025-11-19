select *
from {{ ref('fact_boxscores') }}
    left join {{ source('bronze', 'internal_team_top_players') }}
    on fact_boxscores.team = internal_team_top_players.team
where
    fact_boxscores.team != internal_team_top_players.team
