with my_cte as (
    select
        player,
        season_type,
        p.team,
        t.team as full_team,
        avg_ppg,
        avg_ts_percent,
        prep_contract_value_analysis.avg_mvp_score,
        avg_plus_minus,
        p.games_played,
        ppg_rank,
        scoring_category,
        games_missed,
        penalized_games_missed,
        is_mvp_candidate,
        prep_contract_value_analysis.mvp_rank
    from {{ ref('prep_player_stats') }} as p
        left join {{ ref('prep_contract_value_analysis') }} using (player)
        left join {{ ref('staging_seed_team_attributes') }} as t on p.team = t.team_acronym
    order by avg_mvp_score desc
)

select *
from my_cte

-- where player = 'Justin Robinson'
