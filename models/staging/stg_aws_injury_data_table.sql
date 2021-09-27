SELECT *
FROM {{ source('nba_source', 'aws_injury_data_table')}}