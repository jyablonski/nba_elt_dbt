with my_cte as (
    select
        int_player_stats.player,
        dim_players.headshot as player_logo,
        int_player_stats.season_type,
        int_player_stats.team,
        dim_teams.team as full_team,
        int_player_stats.avg_ppg,
        int_player_stats.avg_ts_percent,
        int_contract_value_analysis.avg_mvp_score,
        int_player_stats.avg_plus_minus,
        int_player_stats.games_played,
        int_player_stats.ppg_rank,
        int_player_stats.scoring_category,
        int_contract_value_analysis.games_missed,
        int_contract_value_analysis.penalized_games_missed,
        int_contract_value_analysis.is_mvp_candidate,
        int_contract_value_analysis.mvp_rank
    from {{ ref('int_player_stats') }} as int_player_stats
        left join {{ ref('int_contract_value_analysis') }} on int_player_stats.player = int_contract_value_analysis.player
        left join {{ ref('dim_teams') }}
            on int_player_stats.team = dim_teams.team_acronym
        left join {{ ref('dim_players') }}
            on int_player_stats.player = dim_players.player
    order by avg_mvp_score desc
)

select *
from my_cte

-- where player = 'Justin Robinson'
