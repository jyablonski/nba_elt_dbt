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
        most_recent_game,
        'join' as join_col

    from {{ ref('prep_bans') }}
),

protocols_data as (
    select
        'join' as join_col,
        sum(active_protocols) as sum_active_protocols
    from {{ ref('prep_standings_table') }}
),

final as (
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
        run_type,
        most_recent_game,
        sum_active_protocols
    from bans_data
        left join protocols_data on bans_data.join_col = protocols_data.join_col
)

select *
from final
