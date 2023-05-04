{{ config(materialized='incremental') }}

-- changed data sources on 2022-07-13, dont have tweet_id, user_id, or profile_img,
with old_twitter as (
	select
		'NA' as tweet_id,
		created_at::date,
		username,
		'0'::int as user_id,
		tweet,
		language,
		likes_count::numeric as likes,
		retweets_count::numeric as retweets,
		scrape_ts,
		'NA' as profile_img,
		link as url,
		compound::numeric,
		neg::numeric,
		neu::numeric,
		pos::numeric,
		sentiment
	from {{ source('nba_source', 'aws_twitter_data_source') }}
),

new_twitter as (
	select 
		tweet_id,
		created_at::date,
		username,
		user_id,
		tweet,
		language,
		likes::numeric,
		retweets::numeric,
		scrape_ts,
		profile_img,
		url,
		compound::numeric,
		neg::numeric,
		neu::numeric,
		pos::numeric,
		sentiment
	from {{ source('nba_source', 'aws_twitter_tweepy_data_source') }}
	{% if is_incremental() %}

	-- this filter will only be applied on an incremental run
	-- only grab records where date is greater than the max date of the existing records in the tablegm
	where scrape_ts > (select max(scrape_ts) from {{ this }})

	{% endif %}
),

final as (
	select *
	from old_twitter
	union
	select *
	from new_twitter
)

select *
from final
