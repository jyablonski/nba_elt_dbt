with twitter_cte as (
    select
        created_at,
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
),

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

select *
from final