with injury_data as (
    select
        player_logo,
        player,
        injury_status,
        continuous_games_missed,
        games_played,
        avg_ppg,
        avg_ts_percent,
        avg_plus_minus,
        avg_mvp_score
    from {{ ref('prep_injury_tracker') }}
    order by avg_mvp_score desc

)

select *
from injury_data
