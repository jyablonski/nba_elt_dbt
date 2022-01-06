with my_cte as (
    select
        *
    from {{ ref('staging_aws_reddit_comment_data_table') }}
),

-- could potentially break down TampaRaptors into Raptors, VanGrizzlies into Grizzlies etc.
top_comments as (
    select
        *,
        ltrim(flair2, ': ') as flair_new,
        case when edited = 'false' then false
            else true end as edited_final,
        {{dbt_utils.split_part('flair2', " ': ' ", 2)}} as flair_new2,
        regexp_replace(flair1, '\d+$', '') as flair_final, --removes trailing digits (Warriors5, Suns2, Bulls1)
        row_number() over (order by score desc) as total_score_rank
    from my_cte
    order by score desc
)

-- 2021-01-05 *** need to create an id field or something and filter out duplicates to grab them by the latest available date
select
    *
from top_comments