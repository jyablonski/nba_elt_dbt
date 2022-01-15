with my_cte as (
    select
        *
    from {{ ref('staging_aws_reddit_comment_data_table') }}
),

aggs as (
    select 
        scrape_date as date,
        count(*) as tot_comments,
        round(avg(compound), 3) as avg_compound,
        round(avg(pos), 3) as avg_pos,
        round(avg(neg), 3) as avg_neg,
        round(avg(neu), 3) as avg_neu
    from my_cte
    group by 1
)

select
    *
from aggs