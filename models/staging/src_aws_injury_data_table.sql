SELECT *
FROM {{ source('nba_prod', 'aws_injury_data_table')}}