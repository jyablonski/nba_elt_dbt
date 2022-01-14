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
        {{ dbt_utils.current_timestamp() }} as last_updated_at,
        '{{ env_var('DBT_PRAC_KEY') }}' as run_type,
        'join' as join_col

    from {{ ref('prep_bans')}}
),

protocols_data as (
    select
        sum(active_protocols) as sum_active_protocols,
        'join' as join_col
    from {{ ref('prep_standings_table')}}
),

protocols_data_lastwk as (
    select
        sum_active_protocols_lastwk,
        'join' as join_col
    from {{ ref('staging_aws_injury_data_table_lastwk')}}
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
        sum_active_protocols,
        sum_active_protocols_lastwk
    from bans_data
    left join protocols_data using (join_col)
    left join protocols_data_lastwk using (join_col)
),

final2 as (
    select 
        *,
        sum_active_protocols_lastwk - sum_active_protocols as protocols_differential,
        round(sum_active_protocols_lastwk - sum_active_protocols / sum_active_protocols_lastwk, 1)::numeric as protocols_pct_diff
    from final
)

select *
from final2