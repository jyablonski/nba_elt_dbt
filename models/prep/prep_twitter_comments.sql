with twitter_cte as (
    select
        *
    from {{ ref('staging_aws_twitter_data_table') }}
),

final as (
    select 
        *,
        row_number() over (order by likes_count desc) as likes_rank_ytd,
        row_number() over (order by retweets_count desc) as retweets_rank_ytd,
        row_number() over (order by replies_count desc) as replies_rank_ytd,
        case when likes_count > 0 then round(replies_count / likes_count, 3)::numeric
        else 0 end as controversial_score
    from twitter_cte
),

-- HANDLING DUPLICATES
-- grabbing the tweet with highest likes_count (assuming this is the most up to date - not most correct way of doing this but w.e)
duplicate_tweets as (
    select
        username,
        tweet,
        scrape_date,
        max(likes_count) as likes_count,
        max(created_at) as created_at
    from twitter_cte
    group by 1, 2, 3
),

final2 as (
    select
        *
    from final
    inner join duplicate_tweets using (username, tweet, scrape_date, likes_count, created_at)
)
-- where username = 'poa_prayer'

select *
from final2
order by likes_count desc