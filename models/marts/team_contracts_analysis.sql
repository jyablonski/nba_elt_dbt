with my_cte as (
    select
        team,
        win_percentage,
        sum_salary_earned,
        sum_salary_earned_max,
        team_pct_salary_earned,
        record,
        value_lost_from_injury,
        team_pct_salary_lost
    from {{ ref('prep_team_contracts_analysis') }}
)

select *
from my_cte
