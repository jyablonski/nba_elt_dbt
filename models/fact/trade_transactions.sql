with transactions as (
    select
        date::date as date,
        transaction::text as transaction,
        scrape_date::date as scrape_date,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_transactions_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from transactions
