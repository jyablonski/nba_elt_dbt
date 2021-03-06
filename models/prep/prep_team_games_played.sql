with my_cte as (
    select distinct
        team,
        date,
        game_id
    from {{ ref('prep_boxscores_mvp_calc') }}
    where type = 'Regular Season'
),

team_gp_counts as (
    select
        team,
        count(*) as team_games_played
    from my_cte
    group by 1
)

select *
from team_gp_counts