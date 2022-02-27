with my_cte as (
    select 
        p.team::text,
        a.team_acronym,
        p.odds::numeric as championship_odds,
        p.predicted::numeric as predicted_wins,
        82 - p.predicted as predicted_losses
    from {{ source('nba_source', 'aws_preseason_odds_source')}} as p
    left join {{ ref('staging_seed_team_attributes')}} as a using (team)
)

select *
from my_cte