with my_cte as (
    select
        team,
        date,
        conference,
        running_total_games_played,
        running_total_wins,
        running_total_losses,
        running_total_win_pct,
        record_as_of_date,
        rank
    from {{ ref('prep_team_record_daily_rollup') }}
)

select *
from my_cte
