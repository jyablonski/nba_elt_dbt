with recent_games as (
    select
        player,
        player_new,
        player_logo,
        outcome,
        team,
        salary,
        pts,
        plusminus,
        game_ts_percent,
        pts_color,
        ts_color

    from {{ ref('prep_recent_games_players')}}
    limit 15
)

select *
from recent_games
