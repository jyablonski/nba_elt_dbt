with my_cte as (
    select
        title::text as title,
        score::integer as score,
        id::text as id,
        url::text as url,
        reddit_url::text as reddit_url,
        num_comments::integer as num_comments,
        body::text as body,
        scrape_date::date as scrape_date,
        scrape_time::timestamp as scrape_time
    from {{ source('nba_source', 'aws_reddit_data_source') }}
)

select *
from my_cte
