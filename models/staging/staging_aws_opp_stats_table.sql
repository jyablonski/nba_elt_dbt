with my_cte as (
    select 
        *,
    row_number() over (order by fg_percent_opp) as fg_percent_rank,
    row_number() over (order by threep_percent_opp) as three_percent_rank,
    row_number() over (order by threep_made_opp) as three_pm_rank
    from {{ source('nba_source', 'aws_opp_stats_source')}}
),

most_recent_date as (
    SELECT
        max(scrape_date) as scrape_date
    FROM {{ source('nba_source', 'aws_opp_stats_source')}}
),

final as (
    select *
    from my_cte
    inner join most_recent_date using (scrape_date)
)


select *
from final