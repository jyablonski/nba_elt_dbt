with player_rolling_avg_aggs as (
    select *
    from {{ ref('prep_player_stats_rolling_avg') }}
),

player_recent_games as (
    select
        c.player,
        c.team,
        s.avg_ppg,
        s.avg_ts_percent,
        s.avg_plus_minus,
        c.avg_mvp_score
    from {{ ref('prep_contract_value_analysis') }} as c
        left join {{ ref('prep_player_stats') }} as s using (player)
    where s.season_type = 'Regular Season'
),

final as (
    select
        *,
        rolling_avg_pts - avg_ppg as ppg_differential,
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_pts - avg_ppg) desc)') }} as ppg_diff_rank,
        rolling_avg_ts_percent - avg_ts_percent as ts_differential,
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_ts_percent - avg_ts_percent) desc)') }} as ts_diff_rank,
        rolling_avg_plus_minus - avg_plus_minus as plus_minus_differential,
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_plus_minus - avg_plus_minus) desc)') }} as plus_minus_diff_rank,
        rolling_avg_mvp_score - avg_mvp_score as mvp_calc_differential, -- notes on this - this is the game mvp calc - adj mvp calc.  slightly lower
        {{ generate_ord_numbers('row_number() over (order by (rolling_avg_mvp_score - avg_mvp_score) desc)') }} as mvp_calc_diff_rank
    from player_rolling_avg_aggs
        left join player_recent_games using (player)
    order by ppg_differential desc
)

select *
from final
