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
        -- these 5 columns are generated from the ingestion script
        compound::numeric as compound,
        neg::numeric as neg,
        neu::numeric as neu,
        pos::numeric as pos,
        sentiment,
        -- `sentiment_category` should be preferred for downstream use. it determines sentiment by looking at the dominant
        -- category for each comment
        case
            when pos::numeric >= neu::numeric and pos::numeric >= neg::numeric then 'Positive'
            when neg::numeric >= neu::numeric and neg::numeric >= pos::numeric then 'Negative'
            else 'Neutral'
        end as sentiment_category,
        regexp_replace(flair1, '\d+$', '') as flair_final, --removes trailing digits (Warriors5, Suns2, Bulls1)
        {{ convert_team_names_flairs("regexp_replace(flair1, '\\d+$', '')") }} as team_flair,
        date(created_at) as created_at_date,
        created_at,
        modified_at
    from {{ source('bronze', 'reddit_comments') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where created_at > (select coalesce(max(created_at), '1900-01-01'::timestamp) from {{ this }})

    {% endif %}
)

select *
from comments
