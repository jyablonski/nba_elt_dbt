with max_date as (
    select max(date) as max_date
    from injury_data
)

select
    injury_data.player,
    injury_data.team,
    teams.team_acronym,
    injury_data.injury,
    injury_data.injury_status,
    injury_data.injury_description
from {{ ref('injury_data') }}
    inner join max_date on injury_data.date = max_date.max_date
    inner join {{ ref('teams') }} on injury_data.team = teams.team
