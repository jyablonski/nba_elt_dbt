with my_cte as (
    select
        player,
        p.team,
        t.team as full_team,
        season_avg_ppg,
        playoffs_avg_ppg,
        season_ts_percent,
        playoffs_ts_percent,
        p.games_played,
        p.games_played_playoffs as playoffs_games_played,
        ppg_rank,
        top20_scorers,
        player_mvp_calc_adj,
        games_missed,
        penalized_games_missed,
        top5_candidates,
        mvp_rank
    from {{ ref('prep_player_aggs') }} as p
        left join {{ ref('prep_contract_value_analysis') }} using (player)
        left join {{ ref('staging_seed_team_attributes') }} as t on p.team = t.team_acronym
    order by player_mvp_calc_adj desc
)

select *
from my_cte

-- where player = 'Justin Robinson'
