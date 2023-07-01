{{ config(materialized='incremental') }}

with my_cte as (
    select distinct
        *
    from {{ ref('staging_aws_twitter_data_table') }}
	{% if is_incremental() %}

	-- this filter will only be applied on an incremental run
	-- only grab records where date is greater than the max date of the existing records in the tablegm
	where date(scrape_ts) > (select max(date) from {{ this }})

	{% endif %}
),

aggs as (
    select 
        date(scrape_ts) as date,
        count(*) as twitter_tot_comments,
        round(avg(compound), 3) as avg_compound,
        round(avg(pos), 3) as avg_pos,
        round(avg(neg), 3) as avg_neg,
        round(avg(neu), 3) as avg_neu,
        'join' as join_col
    from my_cte
    -- where created_at != current_date
    group by 1
),

tot_aggs as (
    select
        avg(twitter_tot_comments) as twitter_avg_comments,
        'join' as join_col
    from aggs
),

final as (
    select
        date,
        twitter_tot_comments,
        avg_compound,
        avg_pos,
        avg_neg,
        avg_neu,
        twitter_avg_comments,
        round(twitter_tot_comments - twitter_avg_comments, 1)::numeric as count_differential,
        round((twitter_tot_comments - twitter_avg_comments) / twitter_avg_comments, 3)::numeric * 100 as twitter_pct_difference
    from aggs
    left join tot_aggs using (join_col)
)

select *
from final