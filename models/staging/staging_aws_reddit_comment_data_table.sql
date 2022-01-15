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
        compound::numeric as compound,
        neg::numeric as neg,
        neu::numeric as neu,
        pos::numeric as pos,
        sentiment
    from {{ source('nba_source', 'aws_reddit_comment_data_source')}}
)

select
    *
from my_cte