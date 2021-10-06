SELECT *
FROM {{ source('nba_source', 'aws_adv_stats_source')}}