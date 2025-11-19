with injury_counts as (
    select
        team,
        count(*) as team_active_injuries,
        0 as team_active_protocols
    from {{ ref('fact_injury_data') }}
    group by team
)

select *
from injury_counts
