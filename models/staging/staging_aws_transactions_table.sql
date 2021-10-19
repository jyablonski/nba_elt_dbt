SELECT
    date::TIMESTAMP AS date,
    transaction::text
FROM
    {{ source('nba_source', 'aws_transactions_source')}}