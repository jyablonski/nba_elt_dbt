with twitter_cte as (
    select
        created_at,
        scrape_ts,
        username,
        tweet,
        url,
        likes,
        retweets,
        compound,
        neg,
        neu,
        pos
    from {{ ref('prep_twitter_comments') }}
    where scrape_ts >= date({{ dbt_utils.current_timestamp() }})
    order by likes desc
    limit 2000
),

/*
recent_date as (
    select distinct 
        created_at
    from twitter_cte
    where created_at >= current_date - 1
),

final as (
    select *
    from twitter_cte
    inner join recent_date using (created_at)
)

*/
final as (
    select
        created_at,
        scrape_ts,
        username,
        tweet,
        url,
        likes,
        retweets,
        compound,
        neg,
        neu,
        pos
    from twitter_cte
)

select *
from final