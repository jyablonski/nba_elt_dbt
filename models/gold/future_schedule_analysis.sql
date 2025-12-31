-- remove as of 2022-04-11 because regular season is over and this isnt needed for postseason.
{{ config(enabled = false) }}
with my_cte as (
    select
        *
    from {{ ref('int_past_schedule_analysis')}}
    where game_status = 'future'
),

team_home_counts as (
    select
        team,
        location_new as home_games_left,
        count(*) as home_games_left_count
    from my_cte
    where location_new = 'home'
    group by
        team,
        location_new

),

team_road_counts as (
    select
        team,
        location_new as road_games_left,
        count(*) as road_games_left_count
    from my_cte
    where location_new = 'road'
    group by
        team,
        location_new

),

team_above_500_counts as (
    select
        team,
        team_status_opp as above_500_games_left,
        count(*) as above_500_games_left_count
    from my_cte
    where team_status_opp = 'Above .500'
    group by
        team,
        team_status_opp
),

team_below_500_counts as (
    select
        team,
        team_status_opp as below_500_games_left,
        count(*) as below_500_games_left_count
    from my_cte
    where team_status_opp = 'Below .500'
    group by
        team,
        team_status_opp
),

team_opp_remaining_win_pct as (
    select
        team,
        round(avg(win_pct_opp), 3)::numeric as avg_win_pct_opp
    from my_cte
    group by team

),

-- this is fucked bc postgres cant use a coalesce column in a previous select command to create a new column.
-- so either split into 2 ctes, or spam coalesce statements
final as (
    select distinct
        m.team,
        coalesce(h.home_games_left_count, 0) as home_games_left_count,
        coalesce(r.road_games_left_count, 0) as road_games_left_count,
        coalesce(a.above_500_games_left_count, 0) as above_500_games_left_count,
        coalesce(b.below_500_games_left_count, 0) as below_500_games_left_count,
        o.avg_win_pct_opp,
        coalesce(h.home_games_left_count, 0)::numeric + coalesce(r.road_games_left_count, 0)::numeric as total_games_left,
        round((coalesce(h.home_games_left_count, 0)::numeric / (coalesce(h.home_games_left_count, 0)::numeric + coalesce(r.road_games_left_count, 0)::numeric)), 3)::numeric as pct_games_left_home,
        round((coalesce(r.road_games_left_count, 0)::numeric / (coalesce(h.home_games_left_count, 0)::numeric + coalesce(r.road_games_left_count, 0)::numeric)), 3)::numeric as pct_games_left_road,
        round((coalesce(a.above_500_games_left_count, 0)::numeric / (coalesce(h.home_games_left_count, 0)::numeric + coalesce(r.road_games_left_count, 0)::numeric)), 3)::numeric as pct_games_left_above_500,
        round((coalesce(b.below_500_games_left_count, 0)::numeric / (coalesce(h.home_games_left_count, 0)::numeric + coalesce(r.road_games_left_count, 0)::numeric)), 3)::numeric as pct_games_left_below_500
    from my_cte as m
    left join team_home_counts as h using (team)
    left join team_road_counts as r using (team)
    left join team_above_500_counts as a using (team)
    left join team_below_500_counts as b using (team)
    left join team_opp_remaining_win_pct as o using (team)

)


select *
from final
