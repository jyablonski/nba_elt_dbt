SELECT *
FROM {{ source('nba_source', 'aws_transactions_table')}}