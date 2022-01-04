with my_cte as (
    select
        *
    from {{ ref('staging_aws_reddit_comment_data_table') }}
),

top_comments as (
    select
        *,
        row_number() over (order by score desc) as total_score_rank
    from my_cte
    order by score desc
)

select
    *
from top_comments