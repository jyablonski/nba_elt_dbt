-- Team MVP production shares should sum to ~100% for each team/season.
select
    team,
    season,
    round(sum(pct_of_team_production)::numeric, 4) as team_production_pct_sum
from {{ ref('player_salary_value') }}
group by
    team,
    season
having abs(sum(pct_of_team_production) - 1) > 0.01
