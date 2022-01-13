with my_cte as (
    select
        *
    from {{ ref('staging_aws_boxscores_table')}}
),

top_players as (
    select
        player,
        team,
        rank as player_rank
    from {{ ref('staging_seed_top_players')}}
),

final as (
    select
        *
    from my_cte
    left join top_players using (player, team)
    where player_rank is not null
),

team_aggs as (
    select 
        team,
        game_id,
        date,
        count(*) as is_top_players
    from final
    group by 1, 2, 3
),

final2 as (
    select distinct
        b.team,
        b.date,
        b.game_id,
        b.outcome,
        b.location,
        b.opponent,
        coalesce(a.is_top_players, 0)::numeric as is_top_players
    from my_cte b
    left join team_aggs a using (team, date, game_id)
    order by team desc, date
)

select *
from final2