{% set rolling_avg_parameter = 6 %}
-- keep in mind for a 7-day rolling average you have to set the rolling_average_parameter to 6 (n -1)

with my_cte as (
    select
        *
    from {{ ref('staging_aws_reddit_comment_data_table') }}
),

aggs as (
    select 
        scrape_date as date,
        count(*) as reddit_tot_comments,
        round(avg(compound), 3) as avg_compound,
        round(avg(pos), 3) as avg_pos,
        round(avg(neg), 3) as avg_neg,
        round(avg(neu), 3) as avg_neu,
        'join' as join_col
    from my_cte
    group by 1
),

tot_aggs as (
    select
        avg(reddit_tot_comments) as reddit_avg_comments,
        'join' as join_col
    from aggs
),

final as (
    select
        date,
        reddit_tot_comments,
        avg_compound,
        avg_pos,
        avg_neg,
        avg_neu,
        reddit_avg_comments,
        round(avg(reddit_tot_comments) over(ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_reddit_comments,
        round(reddit_tot_comments - reddit_avg_comments, 1)::numeric as count_differential,
        round((reddit_tot_comments - reddit_avg_comments) / reddit_avg_comments, 3)::numeric * 100 as reddit_pct_difference
    from aggs
    left join tot_aggs using (join_col)
    order by date
)

select *
from final