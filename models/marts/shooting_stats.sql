with shooting_stats as (
    select
        player,
        fg_pct_0_3,
        fg_pct_3_10,
        fg_pct_10_16,
        fg_pct_16_3p,
        pct_2pfg_ast,
        pct_3pfg_ast,
        dunks
    from {{ ref('prep_shooting_stats') }}
)

select *
from shooting_stats
