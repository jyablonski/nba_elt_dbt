select
    team,
    round(sum(avg_mvp_score), 2) as team_total_mvp_score,
    count(*) as roster_count
from {{ ref('int_contract_value_analysis') }}
group by team
order by team_total_mvp_score desc
