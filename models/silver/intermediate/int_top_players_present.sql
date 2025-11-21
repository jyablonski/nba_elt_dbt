with my_cte as (
    select
        player,
        team,
        game_date,
        location,
        outcome,
        opponent
    from {{ ref('fact_boxscores') }}
),

top_players as (
    select
        player,
        rank as player_rank
    from {{ ref('dim_players') }}
),

final as (
    select *
    from my_cte
        left join top_players using (player)
    where player_rank is not null
),

team_aggs as (
    select
        team,
        game_date,
        count(*) as is_top_players
    from final
    group by
        team,
        game_date
),

final2 as (
    select distinct
        b.team,
        b.game_date,
        b.outcome,
        b.location,
        b.opponent,
        coalesce(a.is_top_players, 0)::numeric as is_top_players
    from my_cte as b
        left join team_aggs as a using (team, game_date)
    order by team desc, game_date asc
)

select *
from final2
