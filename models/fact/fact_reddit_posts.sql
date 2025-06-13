{{ config(materialized='incremental') }}

with posts as (
    select
        title::text as title,
        score::integer as score,
        id::text as id,
        url::text as url,
        reddit_url::text as reddit_url,
        num_comments::integer as num_comments,
        body::text as body,
        scrape_date::date as scrape_date,
        scrape_time::timestamp as scrape_time,
        created_at,
        modified_at
    from {{ source('nba_source', 'reddit_posts') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from posts
