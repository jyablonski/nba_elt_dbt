select
    date,
    transaction
from {{ ref('fact_trade_transactions') }}
order by date
