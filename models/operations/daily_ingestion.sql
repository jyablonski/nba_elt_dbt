with recent_date as (
    select max(scrape_date) as scrape_date
    from {{ ref('staging_aws_reddit_comment_data_table') }}
),

adv_stats_historical as (
    select 
        'adv_stats' as table_name,
        scrape_date,
        count(*) as avg_num_records
    from {{ ref('staging_aws_adv_stats_table') }}
    group by 1, 2
),


adv_stats as (
    select 
        'adv_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_adv_stats_table') }}
    inner join recent_date using (scrape_date)
),

boxscore_stats as (
    select 
        'boxscore_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_boxscores_incremental_table') }} b
    inner join recent_date on b.date = (recent_date.scrape_date - 1) -- -1 because boxscores and pbp are from yesterday
),

injury_stats as (
    select 
        'injury_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_injury_data_table') }} b
    inner join recent_date using (scrape_date)
),

odds_stats as (
    select 
        'odds_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_odds_table') }} b
    inner join recent_date on b.date = recent_date.scrape_date
),

opp_stats as (
    select 
        'opp_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_opp_stats_table') }} b
    inner join recent_date using (scrape_date)
),

pbp_stats as (
    select 
        'pbp_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_pbp_data_table') }} b
    inner join recent_date on b.date = (recent_date.scrape_date - 1) -- -1 because boxscores and pbp are from yesterday
),

reddit_comments_stats as (
    select 
        'reddit_comments_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_reddit_comment_data_table') }} b
    inner join recent_date using (scrape_date)
),

reddit_posts_stats as (
    select 
        'reddit_posts_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_reddit_data_table') }} b
    inner join recent_date using (scrape_date)
),

shooting_stats as (
    select 
        'shooting_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_shooting_stats_table') }} b
    inner join recent_date using (scrape_date)
),

general_stats as (
    select 
        'general_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_stats_table') }} b
    inner join recent_date using (scrape_date)
),

transactions_stats as (
    select 
        'transactions_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_transactions_table') }} b
    inner join recent_date using (scrape_date)
),

twitter_stats as (
    select 
        'twitter_stats' as table_name,
        count(*) as num_records
    from {{ ref('staging_aws_twitter_data_table') }} b
    inner join recent_date on date(b.scrape_ts) = recent_date.scrape_date
),

final as (
    select *
    from adv_stats
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
)

select *
from final