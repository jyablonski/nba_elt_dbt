{{ config(materialized='incremental') }}

with twitter_cte as (
    select
        created_at::timestamp as created_at,
        date::date as date,
        username::text as username,
        tweet::text as tweet,
        language::text as language,
        link::text as url,
        likes_count::numeric as likes_count,
        retweets_count::numeric as retweets_count,
        replies_count::numeric as replies_count,
        scrape_date::date as scrape_date,
        scrape_ts::timestamp as scrape_ts,
        compound::numeric as compound,
        neg::numeric as neg,
        neu::numeric as neu,
        pos::numeric as pos,
        sentiment
    from {{ source('nba_source', 'aws_twitter_data_source') }}
)

select *
from twitter_cte

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- only grab records where date is greater than the max date of the existing records in the tablegm
  where scrape_date > (select max(scrape_date) from {{ this }})

{% endif %}