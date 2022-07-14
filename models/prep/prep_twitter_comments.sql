with twitter_cte as (
    select
        distinct *
    from {{ ref('staging_aws_twitter_data_table') }}
),

-- HANDLING DUPLICATES
-- grabbing the tweet with highest likes (assuming this is the most up to date - not most correct way of doing this but w.e)
duplicate_tweets as (
    select 
        *,
        ROW_NUMBER() over (
            partition by username, tweet, created_at
            order by likes desc
        ) as tweet_rank
    from twitter_cte
),

final as (
    select
        *,
        row_number() over (order by likes desc) as likes_rank_ytd,
        row_number() over (order by retweets desc) as retweets_rank_ytd
    from duplicate_tweets
    where tweet_rank = 1
)
-- where username = 'poa_prayer'
-- where username = 'mehhhaccount'

select *
from final
order by likes desc