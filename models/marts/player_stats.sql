with my_cte as (
    select
        prep_player_stats.player,
        players.headshot as player_logo,
        prep_player_stats.season_type,
        prep_player_stats.team,
        staging_seed_team_attributes.team as full_team,
        prep_player_stats.avg_ppg,
        prep_player_stats.avg_ts_percent,
        prep_contract_value_analysis.avg_mvp_score,
        prep_player_stats.avg_plus_minus,
        prep_player_stats.games_played,
        prep_player_stats.ppg_rank,
        prep_player_stats.scoring_category,
        prep_contract_value_analysis.games_missed,
        prep_contract_value_analysis.penalized_games_missed,
        prep_contract_value_analysis.is_mvp_candidate,
        prep_contract_value_analysis.mvp_rank
    from {{ ref('prep_player_stats') }} as prep_player_stats
        left join {{ ref('prep_contract_value_analysis') }} on prep_player_stats.player = prep_contract_value_analysis.player
        left join {{ ref('teams') }} as staging_seed_team_attributes
            on prep_player_stats.team = staging_seed_team_attributes.team_acronym
        left join {{ ref('players') }}
            on prep_player_stats.player = players.player
    order by avg_mvp_score desc
)

select *
from my_cte

-- where player = 'Justin Robinson'
