with sentiment_time_series as (
    select *
    from {{ ref('prep_reddit_team_sentiment') }}
    where scrape_date >= '2023-10-01'
)

select *
from sentiment_time_series
