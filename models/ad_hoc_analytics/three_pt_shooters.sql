{% set three_pt_parameter = 5 %}

-- simpsons paradox - steph curry has less 3pt % in these games which makes him look "worse" but he has way more of these games than the other players. 
with my_cte as (
    select
        *
    from {{ ref('prep_boxscores_mvp_calc') }}
    where threepfgmade >= {{ three_pt_parameter }}
),

player_gp as (
    select
        player,
        type,
        count(*) as games_played
    from {{ ref('prep_boxscores_mvp_calc') }}
    group by player, type
),

shooter_aggs as (
    select
        player,
        type,
        count(threepfgmade) as num_games_{{ three_pt_parameter }}_three_pters,
        round(avg(threepfgmade::numeric), 3) as avg_three_pters_made,
        round(avg(threepattempted::numeric), 3) as avg_three_pters_attempted,
        round(round(avg(threepfgmade::numeric), 3) / round(avg(threepattempted::numeric), 3), 3)::numeric as three_pt_pct
    from my_cte
    group by player, type
),

final as (
    select 
        *,
        round(num_games_{{ three_pt_parameter }}_three_pters::numeric / games_played::numeric, 3) as pct_games_{{ three_pt_parameter }}_threes
    from shooter_aggs
    left join player_gp g using (player, type)
    order by type, num_games_{{ three_pt_parameter }}_three_pters desc
)

select *
from final
where type = 'Regular Season'