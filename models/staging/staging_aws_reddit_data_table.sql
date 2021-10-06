SELECT title::text as title,
       score::integer as score,
       id::text as id,
       url::text as url,
       num_comments::integer as num_comments,
       body::text as body,
       scrape_date::date as scrape_date,
       scrape_time::timestamp as scrape_time
FROM {{ source('nba_source', 'aws_reddit_data_source')}}