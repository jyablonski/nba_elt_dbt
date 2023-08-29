with recent_games as (
    select
        player,
        player_new,
        player_logo,
        outcome,
        team,
        salary,
        pts,
        game_ts_percent,
        pts_color::text as pts_color,
        ts_color::text as ts_color,
        case
            when plusminus > 0 then concat('+', plusminus::text)::text
            else plusminus::text
        end as plusminus

    from {{ ref('prep_recent_games_players') }}
    limit 15
)

select *
from recent_games
