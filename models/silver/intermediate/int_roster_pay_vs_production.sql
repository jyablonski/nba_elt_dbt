with active_roster as (
    select
        int_player_stats.player,
        int_player_stats.team,
        int_player_stats.games_played,
        coalesce(dim_players.salary, {{ var('default_player_salary') }}) as salary,
        coalesce(dim_players.win_shares, 0) as win_shares,
        dim_players.ws_per_48,
        dim_players.vorp,
        dim_players.pos,
        dim_players.headshot,
        dim_players.contract_team
    from {{ ref('int_player_stats') }}
        inner join {{ ref('int_player_most_recent_team') }}
            on int_player_stats.player = int_player_most_recent_team.player
            and int_player_stats.team = int_player_most_recent_team.team
        left join {{ ref('dim_players') }}
            on int_player_stats.player = dim_players.player
    where int_player_stats.season_type = 'Regular Season'
),

contract_value as (
    select
        player,
        team,
        avg_mvp_score as pvm_avg_mvp_score,
        percentile_rank as pvm_percentile_rank,
        color_var as pvm_color_var,
        salary_rank as pvm_salary_bucket,
        pvm_rank as pvm_salary_bucket_avg,
        games_missed as pvm_games_missed,
        adj_penalty_final as pvm_adj_penalty_final,
        rankingish as pvm_rankingish
    from {{ ref('int_contract_value_analysis') }}
),

valued as (
    select
        active_roster.player,
        active_roster.team,
        active_roster.games_played,
        active_roster.salary,
        active_roster.win_shares,
        active_roster.ws_per_48,
        active_roster.vorp,
        active_roster.pos,
        active_roster.headshot,
        active_roster.contract_team,
        contract_value.pvm_avg_mvp_score,
        contract_value.pvm_percentile_rank,
        contract_value.pvm_color_var,
        contract_value.pvm_salary_bucket,
        contract_value.pvm_salary_bucket_avg,
        contract_value.pvm_games_missed,
        contract_value.pvm_adj_penalty_final,
        contract_value.pvm_rankingish,
        round((active_roster.win_shares * {{ var('dollar_per_win_share') }})::numeric, 0) as market_value,
        round(
            (
                (active_roster.win_shares * {{ var('dollar_per_win_share') }})
                - active_roster.salary
            )::numeric,
            0
        ) as surplus,
        case
            when (active_roster.win_shares * {{ var('dollar_per_win_share') }}) - active_roster.salary > 0
                then 'surplus'
            when (active_roster.win_shares * {{ var('dollar_per_win_share') }}) - active_roster.salary < 0
                then 'deficit'
            else 'even'
        end as value_status
    from active_roster
        left join contract_value
            on active_roster.player = contract_value.player
            and active_roster.team = contract_value.team
)

select
    *,
    row_number() over (
        partition by team
        order by salary desc, player asc
    ) as salary_rank_within_team
from valued
order by team asc, salary desc, player asc
