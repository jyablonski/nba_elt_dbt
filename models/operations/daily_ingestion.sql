with recent_date as (
    select max(scrape_date) as scrape_date
    from {{ ref('reddit_comment_data') }}
),

team_stats as (
    select
        'team_stats' as table_name,
        count(*) as num_records
    from {{ ref('team_adv_stats_data') }}
        inner join recent_date using (scrape_date)
),

boxscore_stats as (
    select
        'boxscore_stats' as table_name,
        count(*) as num_records
    from {{ ref('boxscores') }} as b
        inner join recent_date on b.game_date = (recent_date.scrape_date - 1)
),

injury_stats as (
    select
        'injury_stats' as table_name,
        count(*) as num_records
    from {{ ref('injury_data') }}
        inner join recent_date using (scrape_date)
),

odds_stats as (
    select
        'odds_stats' as table_name,
        count(*) as num_records
    from {{ ref('odds_data') }} as b
        inner join recent_date on b.date = recent_date.scrape_date
),

opp_stats as (
    select
        'opp_stats' as table_name,
        count(*) as num_records
    from {{ ref('opp_stats_data') }}
        inner join recent_date using (scrape_date)
),

pbp_stats as (
    select
        'pbp_stats' as table_name,
        count(*) as num_records
    from {{ ref('pbp_data') }}
        inner join recent_date on pbp_data.game_date = (recent_date.scrape_date - 1) -- -1 because boxscores and pbp are from yesterday
),

reddit_comments_stats as (
    select
        'reddit_comments_stats' as table_name,
        count(*) as num_records
    from {{ ref('reddit_comment_data') }}
        inner join recent_date using (scrape_date)
),

reddit_posts_stats as (
    select
        'reddit_posts_stats' as table_name,
        count(*) as num_records
    from {{ ref('reddit_posts') }}
        inner join recent_date using (scrape_date)
),

shooting_stats as (
    select
        'shooting_stats' as table_name,
        count(*) as num_records
    from {{ ref('shooting_stats_data') }}
        inner join recent_date using (scrape_date)
),

general_stats as (
    select
        'player_stats' as table_name,
        count(*) as num_records
    from {{ ref('player_stats_data') }}
        inner join recent_date using (scrape_date)
),

transactions_stats as (
    select
        'transactions_stats' as table_name,
        count(*) as num_records
    from {{ ref('trade_transactions') }}
        inner join recent_date using (scrape_date)
),

twitter_stats as (
    select
        'twitter_stats' as table_name,
        count(*) as num_records
    from {{ ref('twitter_tweets') }}
        inner join recent_date on date(twitter_tweets.scrape_ts) = recent_date.scrape_date
)

select *
from team_stats
union
select *
from boxscore_stats
union
select *
from injury_stats
union
select *
from odds_stats
union
select *
from opp_stats
union
select *
from pbp_stats
union
select *
from reddit_comments_stats
union
select *
from reddit_posts_stats
union
select *
from shooting_stats
union
select *
from general_stats
union
select *
from transactions_stats
union
select *
from twitter_stats
