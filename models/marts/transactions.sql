select
    date,
    transaction
from {{ ref('trade_transactions') }}
order by date
