select
    word,
    word_frequency,
    frequency_rank,
    nba_team_flairs_using_word,
    top_nba_team_flair,
    most_common_sentiment,
    avg_sentiment_when_used,
    analysis_date
from {{ ref('prep_reddit_most_common_keywords') }}
