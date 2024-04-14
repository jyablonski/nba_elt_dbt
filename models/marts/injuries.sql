select
    injury_data.player,
    injury_data.team,
    teams.team_acronym,
    injury_data.injury,
    injury_data.injury_status,
    injury_data.injury_description,
    injury_data.scrape_date as scrape_date
from {{ ref('injury_data') }}
    inner join {{ ref('teams') }} on injury_data.team = teams.team
