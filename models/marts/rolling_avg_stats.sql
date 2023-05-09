with player_rolling_avg_aggs as (
    select
        *
    from {{ ref('prep_player_stats_rolling_avg') }}
),

player_recent_games as (
    select
        c.player,
        c.team,
        s.season_avg_ppg,
        s.season_ts_percent,
        s.season_avg_plusminus,
        c.player_mvp_calc_adj
    from {{ ref('prep_contract_value_analysis') }} c
    left join {{ ref('prep_player_aggs') }} s using (player)
),

final as (
    select
        *,
        rolling_avg_pts - season_avg_ppg as ppg_differential,
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_pts - season_avg_ppg) desc)') }} as ppg_diff_rank,
        rolling_avg_ts_percent - season_ts_percent as ts_differential,
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_ts_percent - season_ts_percent) desc)') }} as ts_diff_rank,
        rolling_avg_plusminus - season_avg_plusminus as plusminus_differential,
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_plusminus - season_avg_plusminus) desc)') }} as plusminus_diff_rank,
        rolling_avg_mvp_calc - player_mvp_calc_adj as mvp_calc_differential, -- notes on this - this is the game mvp calc - adj mvp calc.  slightly lower
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_mvp_calc - player_mvp_calc_adj) desc)') }} as mvp_calc_diff_rank
    from player_rolling_avg_aggs
    left join player_recent_games using (player)
    order by ppg_differential desc
)

select *
from final