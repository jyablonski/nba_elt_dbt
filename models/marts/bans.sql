with bans_data as (
    select
        upcoming_games::integer as upcoming_games,
        upcoming_game_date::date as upcoming_game_date,
        location::text as location,
        tot_wins::integer as tot_wins,
        games_played::integer as games_played,
        avg_pts::numeric as avg_pts,
        last_yr_ppg::numeric as last_yr_ppg,
        scrape_time::timestamp as scrape_time,
        win_pct::numeric as win_pct,
        league_ts_percent::numeric as league_ts_percent,
        {{ dbt.current_timestamp() }} as last_updated_at,
        most_recent_game
    from {{ ref('prep_bans') }}
),

protocols_data as (
    select sum(active_protocols) as sum_active_protocols
    from {{ ref('prep_standings_table') }}
)

select
    upcoming_games,
    upcoming_game_date,
    location,
    tot_wins,
    games_played,
    avg_pts,
    last_yr_ppg,
    scrape_time,
    win_pct,
    league_ts_percent,
    last_updated_at,
    most_recent_game,
    sum_active_protocols
from bans_data
    cross join protocols_data
