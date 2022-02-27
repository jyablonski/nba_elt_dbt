with my_cte as (
    select *
    from {{ ref('staging_aws_transactions_table') }}
),

most_recent_date as (
    select 
        max(scrape_date) as scrape_date
    from my_cte
),

final as (
    select 
        c.date,
        c.transaction,
        c.scrape_date
    from my_cte as c
    inner join most_recent_date using (scrape_date)
)

select *
from final