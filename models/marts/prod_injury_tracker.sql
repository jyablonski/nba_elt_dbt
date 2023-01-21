with injury_data as (
    select
        player,
        team,
        status,
        injury,
        continuous_games_missed,
        games_played,
        season_avg_ppg,
        player_mvp_calc_avg,
        season_ts_percent,
        season_avg_plusminus
    from {{ ref('prep_injury_tracker') }}
    order by player_mvp_calc_avg desc

)

select *
from injury_data