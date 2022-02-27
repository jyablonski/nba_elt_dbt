with twitter_cte as (
    select
        distinct *
    from {{ ref('staging_aws_twitter_data_table') }}
),

-- HANDLING DUPLICATES
-- grabbing the tweet with highest likes_count (assuming this is the most up to date - not most correct way of doing this but w.e)
duplicate_tweets as (
    select 
        *,
        ROW_NUMBER() over (
            partition by username, tweet, scrape_date
            order by likes_count desc
        ) as tweet_rank
    from twitter_cte
),

final as (
    select
        *,
        row_number() over (order by likes_count desc) as likes_rank_ytd,
        row_number() over (order by retweets_count desc) as retweets_rank_ytd,
        row_number() over (order by replies_count desc) as replies_rank_ytd,
        case when likes_count > 0 then round(replies_count / likes_count, 3)::numeric
        else 0 end as controversial_score
    from duplicate_tweets
    where tweet_rank = 1
)
-- where username = 'poa_prayer'
-- where username = 'mehhhaccount'

select *
from final
order by likes_count desc