with my_cte as (
    SELECT
        title::text AS title,
        score::integer AS score,
        id::text AS id,
        url::text AS url,
        reddit_url::text as reddit_url,
        num_comments::integer AS num_comments,
        body::text AS body,
        scrape_date::date AS scrape_date,
        scrape_time::TIMESTAMP AS scrape_time
    FROM {{ source('nba_source', 'aws_reddit_data_source')}}
)

select *
from my_cte