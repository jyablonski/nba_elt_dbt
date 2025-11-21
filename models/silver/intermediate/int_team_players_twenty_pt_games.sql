with gamelogs as (
    select
        player,
        team,
        count(*) as num_games_over_twenty_pts
    from {{ ref('fact_boxscores') }}
    where pts >= 20
    group by
        player,
        team
    order by
        team,
        count(*) desc
)

select *
from gamelogs
