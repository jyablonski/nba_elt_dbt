SELECT *
FROM {{ source('nba_prod', 'aws_schedule_table')}}