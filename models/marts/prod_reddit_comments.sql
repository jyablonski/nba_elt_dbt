with my_cte as (
    select
        author,
        comment,
        flair_final as flair,
        score,
        url,
        compound,
        pos,
        neu,
        neg,
        scrape_date
    from {{ ref('prep_reddit_comments') }}
),

max_date as (
    select
        max(scrape_date) as scrape_date
    from my_cte

),

final as (
    select
        *
    from my_cte
    inner join max_date using (scrape_date)
    order by score desc
    limit 2000

)

select *
from final
