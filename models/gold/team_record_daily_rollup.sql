with my_cte as (
    select
        team,
        game_date,
        conference,
        running_total_games_played,
        running_total_wins,
        running_total_losses,
        running_total_win_pct,
        record_as_of_date,
        rank
    from {{ ref('int_team_record_daily_rollup') }}
)

select
    *,
    {{ dbt.current_timestamp() }} as __created_at
from my_cte
