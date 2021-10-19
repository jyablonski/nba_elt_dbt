with bans_data as (
    select 
        upcoming_games::integer as upcoming_games,
        upcoming_game_date::date as upcoming_game_date,
        location::text as location,
        tot_wins::integer as tot_wins,
        games_played::integer as games_played,
        avg_pts::numeric as avg_pts,
        last_yr_ppg::numeric as last_yr_ppg,
        win_pct::numeric as win_pct
    from {{ ref('prep_bans')}}
)

select *
from bans_data