select *
from {{ ref('prep_boxscores_mvp_calc') }}
    left join {{ ref('staging_seed_top_players') }}
        on prep_boxscores_mvp_calc.team = staging_seed_top_players.team
where
    prep_boxscores_mvp_calc.team != staging_seed_top_players.team
