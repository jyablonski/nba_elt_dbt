with sentiment_time_series as (
    select *
    from {{ ref('prep_reddit_team_sentiment') }}
)

select *
from sentiment_time_series
