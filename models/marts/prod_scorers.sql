with my_cte as (
    select 
        player,
        team,
        full_team,
        season_avg_ppg,
        playoffs_avg_ppg,
        season_ts_percent,
        playoffs_ts_percent,
        games_played,
        playoffs_games_played,
        ppg_rank,
        top20_scorers,
        player_mvp_calc_adj,
        games_missed,
        penalized_games_missed,
        top5_candidates,
        mvp_rank
    from {{ ref('prep_scorers') }}
    order by player_mvp_calc_adj desc
)

select *
from my_cte
-- where player = 'Justin Robinson'
