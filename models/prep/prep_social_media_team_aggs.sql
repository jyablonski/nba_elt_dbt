with my_cte as (
    select
        author,
        score,
        coalesce(flair_final, 'No Flair')::text as flair,
        compound,
        neg,
        neu,
        pos
    from {{ ref('reddit_comment_data') }}
),

aggs as (
    select
        flair,
        count(*) as num_count,
        round(avg(score), 3) as avg_score,
        round(avg(compound), 3) as avg_compound,
        round(avg(neg), 3) as avg_neg,
        round(avg(neu), 3) as avg_neu,
        round(avg(pos), 3) as avg_pos
    from my_cte
    group by 1
    order by avg_compound desc
)

select *
from aggs
where num_count > 50
