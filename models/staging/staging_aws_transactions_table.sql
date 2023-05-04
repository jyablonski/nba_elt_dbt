{{ config(materialized='incremental') }}

-- 2022-04-06: watchout, date means event_date - should probably rename
with transactions as (
    select
        date::date as date,
        transaction::text as transaction,
        scrape_date::date as scrape_date
    from {{ source('nba_source', 'aws_transactions_source')}}
    {% if is_incremental() %}

      -- this filter will only be applied on an incremental run
      -- only grab records where date is greater than the max date of the existing records in the tablegm
      where scrape_date > (select max(scrape_date) from {{ this }})

    {% endif %}
)

select *
from transactions
