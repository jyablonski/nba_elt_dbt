with max_date as (
    select max(scrape_date) as scrape_date
    from {{ ref('reddit_comment_data') }}

),

comments as (
    select
        author,
        comment,
        flair_final as flair,
        score,
        url,
        compound,
        pos,
        neu,
        neg,
        scrape_date,
        row_number() over (partition by author, comment order by score desc) as row_num
    from {{ ref('reddit_comment_data') }}
        inner join max_date using (scrape_date)
    limit 2000
)


select *
from comments
where row_num = 1
order by score desc
