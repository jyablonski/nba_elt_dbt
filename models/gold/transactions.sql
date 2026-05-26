select
    {{ dbt.current_timestamp() }} as __created_at,
    date,
    transaction
from {{ ref('fact_trade_transactions') }}
order by date
