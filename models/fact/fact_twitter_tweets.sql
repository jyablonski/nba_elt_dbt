{{ config(materialized='incremental') }}

-- 2024-04-11 update: not using `user_id` or `profile_img` at all so removing
-- changed data sources on 2022-07-13, dont have tweet_id, user_id, or profile_img,
with old_twitter_data as (
    select
        'NA' as tweet_id,
        api_created_at::timestamp,
        username,
        tweet,
        language,
        likes_count::numeric as likes,
        retweets_count::numeric as retweets,
        scrape_ts,
        link as url,
        compound::numeric,
        neg::numeric,
        neu::numeric,
        pos::numeric,
        sentiment,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_twitter_data_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where scrape_ts > (select max(scrape_ts) from {{ this }})

    {% endif %}
),

new_twitter_data as (
    select
        tweet_id,
        api_created_at::timestamp,
        username,
        tweet,
        language,
        likes::numeric,
        retweets::numeric,
        scrape_ts,
        url,
        compound::numeric,
        neg::numeric,
        neu::numeric,
        pos::numeric,
        sentiment,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_twitter_tweepy_data_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where scrape_ts > (select max(scrape_ts) from {{ this }})

    {% endif %}
)

select *
from old_twitter_data
union
select *
from new_twitter_data
