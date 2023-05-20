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
        most_recent_game,
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
        most_recent_game,
        sum_active_protocols,
        sum_active_protocols_lastwk
    from bans_data
    left join protocols_data using (join_col)
    left join protocols_data_lastwk using (join_col)
),

final2 as (
    select 
        *,
        abs(sum_active_protocols_lastwk - sum_active_protocols)::numeric as protocols_differential,
        case when sum_active_protocols_lastwk > sum_active_protocols then
            abs(100 * round((sum_active_protocols_lastwk - sum_active_protocols) / sum_active_protocols_lastwk, 3))::numeric
            when sum_active_protocols_lastwk < sum_active_protocols then 
                abs(100 * round((sum_active_protocols - sum_active_protocols_lastwk) / sum_active_protocols, 3))::numeric
            else 0
            end as protocols_pct_diff

    from final
),

final3 as (
    select
        *,
        case when sum_active_protocols > sum_active_protocols_lastwk then concat(protocols_differential, ' More Cases (', round(protocols_pct_diff, 1), '% Increase) from 7 days ago')
             when sum_active_protocols < sum_active_protocols_lastwk then concat(protocols_differential, ' Fewer Cases (', round(protocols_pct_diff, 1), '% Decrease) from 7 days ago')
             else 'No difference from 7 days ago'
             end as protocols_text
    from final2
)

select *
from final3