with transactions_data as (
    select
        date,
        transaction
    from {{ ref('staging_aws_transactions_table')}}
)

select *
from transactions_data
