with twitter_cte as (
    select
        scrape_date,
        username,
        tweet,
        url,
        likes_count,
        retweets_count,
        replies_count,
        compound,
        neg,
        neu,
        pos
    from {{ ref('prep_twitter_comments') }}
),

recent_date as (
    select max(scrape_date) as scrape_date
    from twitter_cte
),

final as (
    select *
    from twitter_cte
    inner join recent_date using (scrape_date)
)

select *
from final