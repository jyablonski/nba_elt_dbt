-- var which adjusts the rolling average time frame
-- this will take an n + 1 rolling average
-- add fg, 3p% as 2 more ?  
{% set rolling_avg_parameter = 9 %}

with my_cte as (
    select
        player,
        game_date,
        pts::numeric,
        game_ts_percent::numeric,
        game_mvp_score::numeric,
        plus_minus::numeric
    from {{ ref('prep_boxscores_mvp_calc') }}
    order by player, game_date
),

-- this will pull up to the last -10- games, not 9.
-- if it's the 6th game of the season, it will pull at 6 games and aggregate those stats.
-- if it's the 11th game, it will pull games 2-11 for a total of 10 games.

cte_aggs as (
    select
        player,
        game_date,
        round(avg(pts) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_pts,
        round(avg(game_ts_percent) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 3)::numeric as rolling_avg_ts_percent,
        round(avg(game_mvp_score) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_mvp_score,
        round(avg(plus_minus) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_plus_minus
    from my_cte
),

max_date as (
    select
        player,
        max(game_date) as game_date
    from cte_aggs
    group by player
),

player_logo as (
    select
        player,
        headshot as player_logo
    from {{ source('nba_source', 'player_attributes') }}
),

final as (
    select *
    from cte_aggs
        inner join max_date using (player, game_date)
),

final2 as (
    select
        final.player,
        player_logo,
        rolling_avg_pts,
        rolling_avg_ts_percent,
        rolling_avg_mvp_score,
        rolling_avg_plus_minus
    from final
        inner join player_logo on final.player = player_logo.player
)

select *
from final2
