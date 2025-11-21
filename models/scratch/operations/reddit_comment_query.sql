{{ config(enabled = false) }}

-- this is a dynamic query i made where you can input a list of strings to query from reddit comments, and then aggregate them by date
-- {% set search_keyword = ['%curry%', '%lebron%', '%draymond%', '%giannis%'] %}

{% set search_keyword = ['%warriors%', '%golden state%', '%gsw%', '%golden state warriors%', '%dubs%'] %}

with my_cte as (
    select *
    from {{ ref('int_reddit_comments') }}
    where comment LIKE ANY(ARRAY {{ search_keyword }} )
),

aggs as (
    select
        scrape_date,
        count(*) as num_comments
    from my_cte
    group by scrape_date
),

final as (
    select
        *
    from aggs
    order by scrape_date desc
)

select *
from final