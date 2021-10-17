with recent_games as (
    SELECT  player,
            player_new,
            player_logo,
            outcome,
            team,
            salary,
            pts,
            game_ts_percent,
            pts_color,
            ts_color

    FROM {{ ref('prep_recent_games_players')}}
    LIMIT 15
)

SELECT *
FROM recent_games