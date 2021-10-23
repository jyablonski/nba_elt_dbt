with my_cte as (
    select
        *
    from {{ ref('prep_team_contracts_analysis')}}
)

select *
from my_cte