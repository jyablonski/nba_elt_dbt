with my_cte as (
    select 
        *
    from {{ source('nba_source', 'aws_opp_stats_source')}}
),

most_recent_date as (
    select
        max(scrape_date) as scrape_date
    from {{ source('nba_source', 'aws_opp_stats_source')}}
    {# where scrape_date = '2022-04-11' -- lock the date to regular season #}
),

final as (
    select 
        *,
        row_number() over (order by fg_percent_opp) as fg_percent_rank,
        row_number() over (order by threep_percent_opp) as three_percent_rank,
        row_number() over (order by threep_made_opp) as three_pm_rank,
        row_number() over (order by ppg_opp) as ppg_opp_rank
    from my_cte
    inner join most_recent_date using (scrape_date)
)


select *
from final