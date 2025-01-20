select
    fact_injury_data.player,
    fact_injury_data.team,
    dim_teams.team_acronym,
    fact_injury_data.injury,
    fact_injury_data.injury_status,
    fact_injury_data.injury_description,
    fact_injury_data.scrape_date
from {{ ref('fact_injury_data') }}
    inner join {{ ref('dim_teams') }} on fact_injury_data.team = dim_teams.team
