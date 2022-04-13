with prod_scorers as (
    select *
    from {{ ref('prep_scorers') }}
),

shooting_stats as (
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
),

final as (
    select *
    from prod_scorers
    left join shooting_stats using (player)
)

select *
from final