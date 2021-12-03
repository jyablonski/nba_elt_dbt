with my_cte as (
    select
        player,
        salary_rank,
        team,
        games_played,
        player_mvp_calc_adj as player_mvp_calc_avg,
        salary,
        team_games_played,
        games_missed,
        pvm_rank,
        rankingish,
        percentile_rank,
        color_var,
        adj_penalty_final,
        1 - adj_penalty_final as pct_penalized
    from {{ ref('prep_contract_value_analysis')}}
)

select *
from my_cte