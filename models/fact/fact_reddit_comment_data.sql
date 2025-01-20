{{ config(materialized='incremental') }}

with comments as (
    select
        author,
        comment,
        score,
        url,
        flair1,
        flair2,
        edited,
        scrape_date,
        scrape_ts,
        compound::numeric as compound,
        neg::numeric as neg,
        neu::numeric as neu,
        pos::numeric as pos,
        sentiment,
        regexp_replace(flair1, '\d+$', '') as flair_final, --removes trailing digits (Warriors5, Suns2, Bulls1)
        date(created_at) as created_at_date,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_reddit_comment_data_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where created_at > (select max(created_at) from {{ this }})

    {% endif %}
)

select *
from comments
