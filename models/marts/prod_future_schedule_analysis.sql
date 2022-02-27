with my_cte as (
    select 
        *
    from {{ ref('prep_past_schedule_analysis')}}
    where game_status = 'future'
),

team_home_counts as (
    select
        team,
        location_new as home_games_left,
        count(*) as home_games_left_count
    from my_cte
    where location_new = 'home'
    group by 1, 2

),

team_road_counts as (
    select
        team,
        location_new as road_games_left,
        count(*) as road_games_left_count
    from my_cte
    where location_new = 'road'
    group by 1, 2

),

team_above_500_counts as (
    select
        team,
        team_status_opp as above_500_games_left,
        count(*) as above_500_games_left_count
    from my_cte
    where team_status_opp = 'Above .500'
    group by 1, 2
),

team_below_500_counts as (
    select
        team,
        team_status_opp as below_500_games_left,
        count(*) as below_500_games_left_count
    from my_cte
    where team_status_opp = 'Below .500'
    group by 1, 2
),

team_opp_remaining_win_pct as (
    select
        team,
        round(avg(win_pct_opp), 3)::numeric as avg_win_pct_opp
    from my_cte
    group by 1

),

final as (
    select distinct
        m.team,
        h.home_games_left_count,
        r.road_games_left_count,
        a.above_500_games_left_count,
        b.below_500_games_left_count,
        o.avg_win_pct_opp,
        h.home_games_left_count::numeric + r.road_games_left_count::numeric as total_games_left,
        round((h.home_games_left_count::numeric / (h.home_games_left_count::numeric + r.road_games_left_count::numeric)), 3)::numeric as pct_games_left_home,
        round((r.road_games_left_count::numeric / (h.home_games_left_count::numeric + r.road_games_left_count::numeric)), 3)::numeric as pct_games_left_road,
        round((a.above_500_games_left_count::numeric / (h.home_games_left_count::numeric + r.road_games_left_count::numeric)), 3)::numeric as pct_games_left_above_500,
        round((b.below_500_games_left_count::numeric / (h.home_games_left_count::numeric + r.road_games_left_count::numeric)), 3)::numeric as pct_games_left_below_500
    from my_cte as m
    left join team_home_counts as h using (team)
    left join team_road_counts as r using (team)
    left join team_above_500_counts as a using (team)
    left join team_below_500_counts as b using (team)
    left join team_opp_remaining_win_pct as o using (team)

)


select *
from final
