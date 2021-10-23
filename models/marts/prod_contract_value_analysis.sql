with my_cte as (
    select
        distinct b.player,
        b.team,
        b.games_played,
        b.player_mvp_calc_avg,
        coalesce(c.salary, 1000000) as salary
    from {{ ref('staging_aws_boxscores_table')}} b
    left join {{ ref('staging_aws_contracts_table')}} c using (player)
    where type = 'Regular Season'
)

select *
from my_cte