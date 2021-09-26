SELECT *
FROM {{ source('nba_prod', 'aws_reddit_data_table')}}