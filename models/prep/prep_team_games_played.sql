with my_cte as (
    select distinct
        team,
        game_date,
        game_id
    from {{ ref('prep_boxscores_mvp_calc') }}
    where season_type = 'Regular Season'
),

team_gp_counts as (
    select
        team,
        count(*) as team_games_played
    from my_cte
    group by team
)

select *
from team_gp_counts
