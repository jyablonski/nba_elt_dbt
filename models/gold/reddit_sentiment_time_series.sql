with sentiment_time_series as (
    select *
    from {{ ref('int_reddit_team_sentiment') }}
    where scrape_date >= '2025-10-01'
)

select
    *,
    {{ dbt.current_timestamp() }} as __created_at
from sentiment_time_series
