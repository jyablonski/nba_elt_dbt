SELECT date::timestamp as date,
       transaction::text
FROM {{ source('nba_source', 'aws_transactions_source')}}