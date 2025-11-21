with twitter_aggs as (
    select
        date,
        twitter_tot_comments,
        twitter_pct_difference
    from {{ ref('int_twitter_aggs') }}
),

reddit_aggs as (
    select
        date,
        reddit_tot_comments,
        reddit_pct_difference
    from {{ ref('int_reddit_aggs') }}
),

max_date as (
    select max(date) as date
    from reddit_aggs
),


final as (
    select
        reddit_aggs.date,
        reddit_tot_comments,
        reddit_pct_difference,
        coalesce(twitter_tot_comments, 0) as twitter_tot_comments,
        coalesce(twitter_pct_difference, 0) as twitter_pct_difference
    from reddit_aggs
        left join twitter_aggs on reddit_aggs.date = twitter_aggs.date
        left join max_date on reddit_aggs.date = max_date.date
    order by date desc
)


select *
from final
    inner join max_date using (date)
