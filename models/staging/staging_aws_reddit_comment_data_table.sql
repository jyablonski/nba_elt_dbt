with my_cte as (
    select
        comment,
        score,
        url,
        scrape_date,
        scrape_ts
    from {{ source('nba_source', 'aws_reddit_comment_data_source')}}
)

select
    *
from my_cte