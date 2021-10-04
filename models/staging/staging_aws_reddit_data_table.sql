SELECT *
FROM {{ source('nba_source', 'aws_reddit_data_source')}}