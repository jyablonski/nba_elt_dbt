with my_cte as (
    select 
        *
    FROM {{ source('nba_source', 'aws_transactions_source')}}
),

most_recent_date as (
    select 
        max(scrape_date) as scrape_date
    FROM {{ source('nba_source', 'aws_transactions_source')}}
),

final as (
    select 
        c.date,
        c.transaction,
        c.scrape_date
    from my_cte c
    inner join most_recent_date using (scrape_date)
)

select *
from final