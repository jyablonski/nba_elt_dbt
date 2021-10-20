with transactions_data as (
    select
        distinct date,
        transaction
    from {{ ref('staging_aws_transactions_table')}}
    order by 1
)

select *
from transactions_data