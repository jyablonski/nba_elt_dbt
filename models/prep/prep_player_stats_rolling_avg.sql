-- var which adjusts the rolling average time frame
-- this will take an n + 1 rolling average
-- add fg, 3p% as 2 more ?  
{% set rolling_avg_parameter = 9 %}

with my_cte as (
    select
        player,
        date,
        pts::numeric,
        game_ts_percent::numeric,
        player_mvp_calc_game::numeric,
        plusminus::numeric
    from {{ ref('prep_boxscores_mvp_calc') }}
    order by player, date
),

-- this will pull up to the last -10- games, not 9.
-- if it's the 6th game of the season, it will pull at 6 games and aggregate those stats.
-- if it's the 11th game, it will pull games 2-11 for a total of 10 games.

cte_aggs as (
    select
        player,
        date::date as date,
        round(avg(pts) over(partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_pts,
        round(avg(game_ts_percent) over(partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 3)::numeric as rolling_avg_ts_percent,
        round(avg(player_mvp_calc_game) over(partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_mvp_calc,
        round(avg(plusminus) over(partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_plusminus
    from my_cte
),

max_date as (
    select 
        player,
        max(date) as date
    from cte_aggs
    group by 1
),

final as (
    select
        *
    from cte_aggs
    inner join max_date using (player, date)
),

final2 as (
    select
        player,
        rolling_avg_pts,
        rolling_avg_ts_percent,
        rolling_avg_mvp_calc,
        rolling_avg_plusminus
    from final
)

select *
from final2
--where player = 'Stephen Curry'
