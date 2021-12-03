with my_cte as (
    select
        *,
        case when url LIKE '%twitter%' then 'Twitter'
        when url LIKE '%streamable%' then 'Streamable'
        when url LIKE '%reddit%' then 'Reddit Text Post'
        else 'Unclassified' end as post_type
    from {{ ref('staging_aws_reddit_data_table') }}
),

aggs as (
    select
        post_type,
        round(avg(score), 1)::numeric as avg_score,
        round(avg(num_comments), 1)::numeric as avg_num_comments
    from my_cte
    group by 1
)

select *
from my_cte
left join aggs using (post_type)
order by score desc