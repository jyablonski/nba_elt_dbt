SELECT *
FROM {{ source('nba_source', 'aws_stats_source')}}