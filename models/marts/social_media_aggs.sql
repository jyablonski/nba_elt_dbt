with twitter_aggs as (
    select
        date,
        twitter_tot_comments,
        twitter_pct_difference
    from {{ ref('prep_twitter_aggs') }}
),

reddit_aggs as (
    select
        date,
        reddit_tot_comments,
        reddit_pct_difference
    from {{ ref('prep_reddit_aggs') }}
),

final as (
    select
        twitter_aggs.date,
        reddit_tot_comments,
        reddit_pct_difference,
        twitter_tot_comments,
        twitter_pct_difference
    from twitter_aggs
        left join reddit_aggs using (date)
    order by date desc
),

max_date as (
    select max(date) as date
    from final
)

select *
from final
    inner join max_date using (date)
