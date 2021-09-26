SELECT *
FROM {{ source('nba_prod', 'aws_stats_table')}}