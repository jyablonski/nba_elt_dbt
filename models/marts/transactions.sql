with transactions_data as (
    select
        date,
        transaction
    from {{ ref('prep_transactions') }}
    order by 1
)

select *
from transactions_data
