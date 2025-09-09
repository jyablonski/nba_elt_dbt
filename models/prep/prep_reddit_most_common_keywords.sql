{{ config(materialized='table') }}

with latest_day as (
    select max(created_at_date) as max_date
    from {{ ref('fact_reddit_comment_data') }}
),

latest_comments as (
    select
        comment,
        compound,
        sentiment_category,
        team_flair,
        created_at_date
    from {{ ref('fact_reddit_comment_data') }}
    where created_at_date = (select max_date from latest_day)
),

-- Split comments into individual words
word_split as (
    select
        team_flair,
        sentiment_category,
        compound,
        created_at_date,
        -- Split on whitespace and punctuation, convert to lowercase, and trim whitespace
        lower(trim(regexp_split_to_table(
            regexp_replace(comment, '[^\w\s]', ' ', 'g'),
            '\s+'
        ))) as word
    from latest_comments
),

-- Filter out common stop words and short words
filtered_words as (
    select
        -- Additional trim to ensure no leading/trailing spaces
        trim(word) as word,
        team_flair,
        sentiment_category,
        compound,
        created_at_date
    from word_split
    where
        length(trim(word)) >= 3  -- Use trimmed length
        and trim(word) != ''     -- Exclude empty strings after trimming
        and word not in (
            -- Original stop words
            'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how', 'man', 'new', 'now', 'old', 'see', 'two', 'way', 'who', 'boy', 'did', 'its', 'let', 'put', 'say', 'she', 'too', 'use',
            'that', 'with', 'have', 'this', 'will', 'your', 'from', 'they', 'know', 'want', 'been', 'good', 'much', 'some', 'time', 'very', 'when', 'come', 'here', 'just', 'like', 'long', 'make', 'many', 'over', 'such', 'take', 'than', 'them', 'well', 'were', 'what',
            'there', 'would', 'about', 'after', 'again', 'before', 'being', 'could', 'every', 'first', 'going', 'great', 'might', 'never', 'other', 'right', 'should', 'still', 'their', 'these', 'think', 'three', 'where', 'which', 'while', 'years',
            'because', 'better', 'little', 'people', 'really', 'should', 'through', 'without', 'another', 'between', 'nothing', 'someone', 'something', 'together',
            -- Common filler/connector words
            'more', 'then', 'also', 'only', 'even', 'back', 'down', 'into', 'most', 'same', 'away', 'next', 'look', 'give', 'made', 'said', 'does', 'getting', 'enough', 'maybe', 'since', 'those', 'around', 'under', 'always', 'ever', 'though', 'already', 'saying',
            -- Negations and contractions
            'don', 'doesn', 'didn', 'isn', 'wasn', 'wouldn', 'gonna', 'won', 'aren', 'dont',
            -- Generic qualifiers
            'any', 'big', 'bad', 'hard', 'real', 'pretty', 'sure', 'probably', 'actually', 'maybe', 'everyone', 'anyone', 'anything', 'everything', 'less', 'high', 'different', 'true', 'young', 'free', 'fine', 'rich',
            -- Vague references
            'guy', 'guys', 'thing', 'lot', 'top', 'post', 'makes', 'look', 'mean', 'dude', 'things', 'reason', 'level', 'amount', 'evidence',
            -- Generic NBA terms (too common to be meaningful)
            'nba', 'team', 'teams', 'player', 'players', 'game', 'games', 'season', 'league', 'basketball', 'play', 'fans',
            -- URLs and web artifacts
            'https', 'com',
            -- Additional filler/connector words
            'why', 'got', 'off', 'need', 'doing', 'few', 'both', 'own', 'least', 'keep', 'part', 'yes', 'else', 'seems', 'trying', 'either', 'whole', 'almost', 'sense', 'absolutely', 'yeah', 'basically', 'instead', 'anymore',
            -- Generic verbs/actions
            'win', 'run', 'gets', 'feel', 'work', 'show', 'pick', 'played', 'playing', 'making', 'talking', 'believe',
            -- Internet slang
            'literally', 'lmao', 'lol'
        )
        and word ~ '^[a-zA-Z]+$' -- only alphabetic characters
),

-- Aggregate word counts with additional metrics
word_stats as (
    select
        word,
        count(*) as word_frequency,
        -- Count distinct NBA team flairs only (max 30)
        count(distinct case
            when team_flair in (
                    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
                    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
                    'OKC', 'ORL', 'POR', 'PHI', 'PHX', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
                )
                then team_flair
        end) as nba_team_flairs_using_word,
        -- Most common NBA team flair for this word
        mode() within group (
            order by
                case
                    when team_flair in (
                            'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
                            'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
                            'OKC', 'ORL', 'POR', 'PHI', 'PHX', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
                        )
                        then team_flair
                end
        ) as top_nba_team_flair,
        avg(compound) as avg_sentiment_when_used,
        mode() within group (order by sentiment_category) as most_common_sentiment,
        max(created_at_date) as analysis_date
    from filtered_words
    group by word
),

-- Add rankings
final_stats as (
    select
        *,
        row_number() over (order by word_frequency desc) as frequency_rank
    from word_stats
)

select
    word,
    word_frequency,
    frequency_rank,
    nba_team_flairs_using_word,
    coalesce(top_nba_team_flair, 'NO_TEAM_FLAIR') as top_nba_team_flair,
    most_common_sentiment,
    round(avg_sentiment_when_used, 3) as avg_sentiment_when_used,
    analysis_date
from final_stats
order by word_frequency desc
limit 100
