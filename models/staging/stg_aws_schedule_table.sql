SELECT *
FROM {{ source('nba_source', 'aws_schedule_table')}}