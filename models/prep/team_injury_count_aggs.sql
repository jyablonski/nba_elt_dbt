with injury_counts as (
    select
        team,
        count(*) as team_active_injuries
    from {{ ref('injury_data') }}
    group by team
)

select *
from injury_counts
