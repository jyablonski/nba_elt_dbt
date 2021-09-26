SELECT *
FROM {{ source('nba_prod', 'aws_adv_stats_table')}}