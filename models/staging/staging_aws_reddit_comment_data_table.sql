with my_cte as (
    select
        author,
        comment,
        score,
        url,
        flair1,
        flair2,
        edited,
        scrape_date,
        scrape_ts,
        compound,
        neg,
        neu,
        pos,
        sentiment
    from {{ source('nba_source', 'aws_reddit_comment_data_source')}}
)

select
    *
from my_cte