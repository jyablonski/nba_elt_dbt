with my_cte as (
    select
        *
    from {{ ref('prep_contract_value_analysis')}}
)

select *
from my_cte