with my_cte as (
    select
        opp_stats_data.team,
        conference,
        scrape_date,
        fg_percent_opp,
        threep_percent_opp,
        threep_made_opp,
        ppg_opp,
        fg_percent_rank,
        three_percent_rank,
        three_pm_rank,
        ppg_opp_rank
    from {{ ref('opp_stats_data') }}
        left join {{ ref('teams') }} on opp_stats_data.team = teams.team
)

select
    team,
    scrape_date,
    fg_percent_opp,
    threep_percent_opp,
    threep_made_opp,
    ppg_opp,
    {{ generate_ord_numbers('fg_percent_rank') }} as fg_percent_rank,
    {{ generate_ord_numbers('three_percent_rank') }} as three_percent_rank,
    {{ generate_ord_numbers('three_pm_rank') }} as three_pm_rank,
    {{ generate_ord_numbers('ppg_opp_rank') }} as ppg_opp_rank,
    case
        when conference = 'Eastern' then 'East'
        else 'West'
    end as conference
from my_cte
