with most_recent_date as (
    select max(game_date) as most_recent_date
    from {{ ref('int_player_stats_rolling_avg') }}
),

player_rolling_avg_aggs as (
    select
        *,
        {{ generate_ord_numbers('row_number() over (order by ppg_diff desc)') }} as ppg_diff_rank,
        {{ generate_ord_numbers('row_number() over (order by ts_percent_diff desc)') }} as ts_diff_rank,
        {{ generate_ord_numbers('row_number() over (order by mvp_score_diff desc)') }} as mvp_calc_diff_rank,
        {{ generate_ord_numbers('row_number() over (order by plus_minus_diff desc)') }} as plus_minus_diff_rank,
        row_number() over (partition by player order by game_date desc) as most_recent_game
    from {{ ref('int_player_stats_rolling_avg') }}
        inner join most_recent_date on true -- Cartesian join to bring in the most recent date
    where game_date >= most_recent_date.most_recent_date - interval '14 days' -- Filter records within 14 days of the most recent date
)


select
    player,
    game_date,
    rolling_avg_pts,
    rolling_avg_ts_percent,
    rolling_avg_mvp_score,
    rolling_avg_plus_minus,
    avg_ppg,
    avg_ts_percent,
    avg_mvp_score,
    avg_plus_minus,
    ppg_diff,
    ts_percent_diff,
    mvp_score_diff,
    plus_minus_diff,
    max_game_date,
    ppg_diff_rank,
    ts_diff_rank,
    mvp_calc_diff_rank,
    plus_minus_diff_rank
from player_rolling_avg_aggs
where most_recent_game = 1
