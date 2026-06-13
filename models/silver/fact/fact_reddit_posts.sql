{{
    config(
        materialized='incremental',
        unique_key=['reddit_url', 'scrape_date']
    )
}}

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
    from {{ source('bronze', 'reddit_posts') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select coalesce(max(modified_at), '1900-01-01'::timestamp) from {{ this }})

    {% endif %}
),

deduped_posts as (
    select
        posts.*,
        row_number() over (
            partition by posts.reddit_url, posts.scrape_date
            order by
                posts.created_at desc nulls last,
                posts.modified_at desc nulls last
        ) as row_num
    from posts
)

select
    title,
    score,
    id,
    url,
    reddit_url,
    num_comments,
    body,
    scrape_date,
    scrape_time,
    created_at,
    modified_at
from deduped_posts
where row_num = 1
