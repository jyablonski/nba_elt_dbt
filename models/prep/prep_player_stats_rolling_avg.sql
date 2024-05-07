-- var which adjusts the rolling average time frame
-- this will take an n + 1 rolling average
-- add fg, 3p% as 2 more ?  
{% set rolling_avg_parameter = 9 %}

with player_boxscores as (
    select
        player,
        game_date,
        pts::numeric,
        game_ts_percent::numeric,
        game_mvp_score::numeric,
        plus_minus::numeric
    from {{ ref('boxscores') }}
    order by
        player,
        game_date
),

-- this will pull up to the last -10- games, not 9.
-- if it's the 6th game of the season, it will pull at 6 games and aggregate those stats.
-- if it's the 11th game, it will pull games 2-11 for a total of 10 games.

player_rolling_avg as (
    select
        player,
        game_date,
        round(avg(pts) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_pts,
        round(avg(game_ts_percent) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 3)::numeric as rolling_avg_ts_percent,
        round(avg(game_mvp_score) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_mvp_score,
        round(avg(plus_minus) over (partition by player ROWS BETWEEN '{{rolling_avg_parameter}}' PRECEDING AND CURRENT ROW), 1)::numeric as rolling_avg_plus_minus
    from player_boxscores
),

max_player_game_date as (
    select
        player,
        max(game_date) as max_game_date
    from player_boxscores
    group by player
),

final as (
    select
        player_rolling_avg.player,
        player_rolling_avg.game_date,
        player_rolling_avg.rolling_avg_pts,
        player_rolling_avg.rolling_avg_ts_percent,
        player_rolling_avg.rolling_avg_mvp_score,
        player_rolling_avg.rolling_avg_plus_minus,
        prep_player_stats.avg_ppg,
        prep_player_stats.avg_ts_percent,
        prep_player_stats.avg_mvp_score,
        prep_player_stats.avg_plus_minus,
        player_rolling_avg.rolling_avg_pts - prep_player_stats.avg_ppg as ppg_diff,
        player_rolling_avg.rolling_avg_ts_percent - prep_player_stats.avg_ts_percent as ts_percent_diff,
        player_rolling_avg.rolling_avg_mvp_score - prep_player_stats.avg_mvp_score as mvp_score_diff,
        player_rolling_avg.rolling_avg_plus_minus - prep_player_stats.avg_plus_minus as plus_minus_diff,
        max_player_game_date.max_game_date
    from player_rolling_avg
        inner join {{ ref('prep_player_stats') }} on player_rolling_avg.player = prep_player_stats.player
        inner join max_player_game_date on player_rolling_avg.player = max_player_game_date.player
    where prep_player_stats.season_type = 'Regular Season'
)

select *
from final
