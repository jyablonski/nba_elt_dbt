{% set rolling_avg_days = 7 %}
{# Note: Window function uses (n-1), so 7-day average needs 6 preceding rows #}

with base_comments as (
    select distinct
        scrape_date,
        compound,
        pos,
        neg,
        neu
    from {{ ref('fact_reddit_comment_data') }}
),

daily_aggregates as (
    select
        scrape_date as date,
        count(*) as reddit_tot_comments,
        round(avg(compound), 3) as avg_compound,
        round(avg(pos), 3) as avg_pos,
        round(avg(neg), 3) as avg_neg,
        round(avg(neu), 3) as avg_neu
    from base_comments
    group by scrape_date
),

overall_average as (
    select avg(reddit_tot_comments) as reddit_avg_comments
    from daily_aggregates
),

final as (
    select
        daily_aggregates.date,
        daily_aggregates.reddit_tot_comments,
        daily_aggregates.avg_compound,
        daily_aggregates.avg_pos,
        daily_aggregates.avg_neg,
        daily_aggregates.avg_neu,
        overall_average.reddit_avg_comments,
        round(
            avg(daily_aggregates.reddit_tot_comments) over (
                order by daily_aggregates.date
                rows between {{ rolling_avg_days - 1 }} preceding and current row
            ),
            1
        ) as rolling_avg_reddit_comments,
        round(
            daily_aggregates.reddit_tot_comments - overall_average.reddit_avg_comments,
            1
        ) as count_differential,
        round(
            (daily_aggregates.reddit_tot_comments - overall_average.reddit_avg_comments)
            / overall_average.reddit_avg_comments * 100,
            3
        ) as reddit_pct_difference
    from daily_aggregates
        cross join overall_average
    order by daily_aggregates.date
)

select *
from final
