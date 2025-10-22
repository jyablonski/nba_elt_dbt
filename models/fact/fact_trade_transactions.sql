with transactions as (
    select
        date::date as date,
        transaction::text as transaction,
        scrape_date::date as scrape_date,
        created_at,
        modified_at
    from {{ source('nba_source', 'bbref_league_transactions') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where created_at > (select coalesce(max(modified_at), '1900-01-01'::timestamp) from {{ this }})

    {% endif %}
)

select *
from transactions
