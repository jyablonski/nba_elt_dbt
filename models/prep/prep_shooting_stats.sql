with my_cte as (
    select *
    from {{ ref('staging_aws_shooting_stats_table') }}
),

max_date as (
    select max(scrape_date) as scrape_date
    from my_cte
),

final as (
    select *
    from my_cte
        inner join max_date using (scrape_date)
)

select *
from final
