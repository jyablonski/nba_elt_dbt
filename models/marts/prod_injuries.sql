with my_cte as (
    select
        *
    from {{ ref('staging_aws_injury_data_table')}}
)

select *
from my_cte