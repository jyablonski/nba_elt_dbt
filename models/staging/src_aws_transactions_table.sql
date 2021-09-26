SELECT *
FROM {{ source('nba_prod', 'aws_transactions_table')}}