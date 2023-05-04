with opp_stats_cte as (
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

-- teams that made the playoffs were getting an asterisk suffix attached, so i filter it out.
final as (
    select 
        opp_stats_cte.scrape_date,
        replace(opp_stats_cte.team, '*', '') as team,
        opp_stats_cte.fg_percent_opp,
        opp_stats_cte.threep_percent_opp,
        opp_stats_cte.threep_made_opp,
        opp_stats_cte.ppg_opp,
        row_number() over (order by opp_stats_cte.fg_percent_opp) as fg_percent_rank,
        row_number() over (order by opp_stats_cte.threep_percent_opp) as three_percent_rank,
        row_number() over (order by opp_stats_cte.threep_made_opp) as three_pm_rank,
        row_number() over (order by opp_stats_cte.ppg_opp) as ppg_opp_rank
    from opp_stats_cte
    inner join most_recent_date using (scrape_date)
)


select *
from final