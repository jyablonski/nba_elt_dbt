with my_cte as (
    select 
        distinct team,
        win_pct,
        avg_win_pct_opp,
        home_record,
        road_record,
        above_record,
        below_record,
        pct_vs_above_500,
        pct_vs_below_500,
        record
    from {{ ref('prep_past_schedule_analysis')}}
)

select *
from my_cte