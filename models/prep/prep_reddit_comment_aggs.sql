with my_cte as (
    select
        *
    from {{ ref('staging_aws_reddit_comment_data_table') }}
),

aggs as (
    select 
        scrape_date as date,
        count(*) as tot_comments
    from my_cte
    group by 1
)

select
    *
from aggs