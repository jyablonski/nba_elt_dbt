{{
    config(
        materialized='table',
        tags=['GOLD', 'OPS', 'DAILY']
    )
}}

/*
Daily ops mart: one row per dashboard gold table. Combines source ingestion
activity with the gold table's persisted __created_at timestamp.
*/

with check_context as (
    select
        ({{ dbt.current_timestamp() }} at time zone '{{ var("dbt_date:time_zone") }}')::date as check_date,
        {{ dbt.current_timestamp() }} as checked_at
),

enabled_flags as (
    select
        id,
        flag
    from {{ source('gold', 'feature_flags') }}
    where is_enabled = 1
),

-- 1:1 with ingestion write_to_sql / write_to_sql_upsert targets in ingestion.
flag_bronze_map as (
    select *
    from (
        values
            ('stats', 'bbref_player_stats_snapshot', 'append'),
            ('boxscores', 'bbref_player_boxscores', 'upsert'),
            ('injuries', 'bbref_player_injuries', 'upsert'),
            ('transactions', 'bbref_league_transactions', 'upsert'),
            ('player_adv_stats', 'bbref_player_adv_stats', 'upsert'),
            ('adv_stats', 'bbref_team_adv_stats_snapshot', 'append'),
            ('shooting_stats', 'bbref_player_shooting_stats', 'upsert'),
            ('odds', 'draftkings_game_odds', 'upsert'),
            ('reddit_posts', 'reddit_posts', 'upsert'),
            ('reddit_comments', 'reddit_comments', 'upsert'),
            ('pbp', 'bbref_player_pbp', 'upsert'),
            ('schedule', 'bbref_league_schedule', 'upsert'),
            ('opp_stats', 'bbref_team_opponent_shooting_stats', 'upsert')
    ) as mapping (flag, bronze_source_table, write_method)
),

gold_table_source_map as (
    select *
    from (
        values
            ('bans', 'schedule'),
            ('bans', 'boxscores'),
            ('bans', 'injuries'),
            ('bans', 'reddit_posts'),
            ('contract_value_analysis', 'player_adv_stats'),
            ('contract_value_analysis', 'boxscores'),
            ('game_types', 'boxscores'),
            ('injuries', 'injuries'),
            ('injury_tracker', 'player_adv_stats'),
            ('injury_tracker', 'boxscores'),
            ('injury_tracker', 'injuries'),
            ('mov', 'schedule'),
            ('mov', 'boxscores'),
            ('mov', 'injuries'),
            ('mov', 'odds'),
            ('opp_stats', 'opp_stats'),
            ('past_schedule_analysis', 'schedule'),
            ('past_schedule_analysis', 'boxscores'),
            ('past_schedule_analysis', 'injuries'),
            ('past_schedule_analysis', 'odds'),
            ('pbp', 'pbp'),
            ('player_stats', 'player_adv_stats'),
            ('player_stats', 'boxscores'),
            ('preseason_odds', 'boxscores'),
            ('preseason_odds', 'injuries'),
            ('recent_games_players', 'player_adv_stats'),
            ('recent_games_players', 'boxscores'),
            ('recent_games_teams', 'boxscores'),
            ('recent_games_teams', 'pbp'),
            ('reddit_comments', 'reddit_comments'),
            ('reddit_recent_keywords', 'reddit_comments'),
            ('reddit_sentiment_time_series', 'boxscores'),
            ('reddit_sentiment_time_series', 'reddit_comments'),
            ('rolling_avg_stats', 'boxscores'),
            ('schedule_season_remaining', 'schedule'),
            ('schedule_season_remaining', 'boxscores'),
            ('schedule_season_remaining', 'injuries'),
            ('schedule_season_remaining', 'odds'),
            ('social_media_aggs', 'reddit_comments'),
            ('standings', 'boxscores'),
            ('standings', 'injuries'),
            ('team_adv_stats', 'adv_stats'),
            ('team_blown_leads', 'pbp'),
            ('team_contracts_analysis', 'schedule'),
            ('team_contracts_analysis', 'player_adv_stats'),
            ('team_contracts_analysis', 'boxscores'),
            ('team_contracts_analysis', 'injuries'),
            ('team_contracts_analysis', 'odds'),
            ('team_odds_outcomes', 'boxscores'),
            ('team_odds_outcomes', 'odds'),
            ('team_ratings', 'adv_stats'),
            ('team_record_daily_rollup', 'boxscores'),
            ('transactions', 'transactions'),
            ('schedule_tonights_games', 'schedule'),
            ('schedule_tonights_games', 'boxscores'),
            ('schedule_tonights_games', 'injuries'),
            ('schedule_tonights_games', 'odds')
    ) as mapping (gold_table, flag)
),

dashboard_gold_tables as (
    select *
    from (
        values
            ('bans'),
            ('contract_value_analysis'),
            ('game_types'),
            ('injuries'),
            ('injury_tracker'),
            ('mov'),
            ('opp_stats'),
            ('past_schedule_analysis'),
            ('pbp'),
            ('player_stats'),
            ('preseason_odds'),
            ('recent_games_players'),
            ('recent_games_teams'),
            ('reddit_comments'),
            ('reddit_recent_keywords'),
            ('reddit_sentiment_time_series'),
            ('rolling_avg_stats'),
            ('schedule_season_remaining'),
            ('social_media_aggs'),
            ('standings'),
            ('team_adv_stats'),
            ('team_blown_leads'),
            ('team_contracts_analysis'),
            ('team_odds_outcomes'),
            ('team_ratings'),
            ('team_record_daily_rollup'),
            ('transactions'),
            ('schedule_tonights_games')
    ) as tables (gold_table)
),

bronze_today as (
    {{ bronze_ingestion_check('bbref_player_stats_snapshot') }}
    union all
    {{ bronze_ingestion_check('bbref_player_boxscores') }}
    union all
    {{ bronze_ingestion_check('bbref_player_injuries') }}
    union all
    {{ bronze_ingestion_check('bbref_league_transactions') }}
    union all
    {{ bronze_ingestion_check('bbref_player_adv_stats') }}
    union all
    {{ bronze_ingestion_check('bbref_team_adv_stats_snapshot') }}
    union all
    {{ bronze_ingestion_check('bbref_player_shooting_stats') }}
    union all
    {{ bronze_ingestion_check('draftkings_game_odds') }}
    union all
    {{ bronze_ingestion_check('reddit_posts') }}
    union all
    {{ bronze_ingestion_check('reddit_comments') }}
    union all
    {{ bronze_ingestion_check('bbref_player_pbp') }}
    union all
    {{ bronze_ingestion_check('bbref_league_schedule') }}
    union all
    {{ bronze_ingestion_check('bbref_team_opponent_shooting_stats') }}
),

source_health as (
    select
        flag_bronze_map.flag,
        flag_bronze_map.bronze_source_table,
        flag_bronze_map.write_method,
        coalesce(bronze_today.records_today, 0) as records_today,
        bronze_today.latest_activity_at,
        coalesce(bronze_today.records_today, 0) > 0 as is_source_fresh
    from flag_bronze_map
        inner join enabled_flags
            on flag_bronze_map.flag = enabled_flags.flag
        left join bronze_today
            on flag_bronze_map.bronze_source_table = bronze_today.bronze_source_table
),

gold_source_health as (
    select
        gold_table_source_map.gold_table,
        count(distinct source_health.bronze_source_table) as mapped_source_count,
        count(distinct source_health.bronze_source_table) filter (where source_health.is_source_fresh) as fresh_source_count,
        coalesce(sum(source_health.records_today), 0) as source_records_today,
        max(source_health.latest_activity_at) as latest_source_activity_at,
        array_to_string(array_agg(distinct source_health.flag order by source_health.flag), ', ') as source_flags,
        array_to_string(
            array_agg(distinct source_health.bronze_source_table order by source_health.bronze_source_table),
            ', '
        ) as bronze_source_tables,
        coalesce(bool_or(source_health.is_source_fresh), false) as is_source_fresh
    from gold_table_source_map
        left join source_health
            on gold_table_source_map.flag = source_health.flag
    group by gold_table_source_map.gold_table
),

gold_table_stats as (
    select 'bans' as gold_table, count(*) as gold_row_count, max(__created_at) as gold_refreshed_at from {{ ref('bans') }}
    union all
    select 'contract_value_analysis', count(*), max(__created_at) from {{ ref('contract_value_analysis') }}
    union all
    select 'game_types', count(*), max(__created_at) from {{ ref('game_types') }}
    union all
    select 'injuries', count(*), max(__created_at) from {{ ref('injuries') }}
    union all
    select 'injury_tracker', count(*), max(__created_at) from {{ ref('injury_tracker') }}
    union all
    select 'mov', count(*), max(__created_at) from {{ ref('mov') }}
    union all
    select 'opp_stats', count(*), max(__created_at) from {{ ref('opp_stats') }}
    union all
    select 'past_schedule_analysis', count(*), max(__created_at) from {{ ref('past_schedule_analysis') }}
    union all
    select 'pbp', count(*), max(__created_at) from {{ ref('pbp') }}
    union all
    select 'player_stats', count(*), max(__created_at) from {{ ref('player_stats') }}
    union all
    select 'preseason_odds', count(*), max(__created_at) from {{ ref('preseason_odds') }}
    union all
    select 'recent_games_players', count(*), max(__created_at) from {{ ref('recent_games_players') }}
    union all
    select 'recent_games_teams', count(*), max(__created_at) from {{ ref('recent_games_teams') }}
    union all
    select 'reddit_comments', count(*), max(__created_at) from {{ ref('reddit_comments') }}
    union all
    select 'reddit_recent_keywords', count(*), max(__created_at) from {{ ref('reddit_recent_keywords') }}
    union all
    select 'reddit_sentiment_time_series', count(*), max(__created_at) from {{ ref('reddit_sentiment_time_series') }}
    union all
    select 'rolling_avg_stats', count(*), max(__created_at) from {{ ref('rolling_avg_stats') }}
    union all
    select 'schedule_season_remaining', count(*), max(__created_at) from {{ ref('schedule_season_remaining') }}
    union all
    select 'social_media_aggs', count(*), max(__created_at) from {{ ref('social_media_aggs') }}
    union all
    select 'standings', count(*), max(__created_at) from {{ ref('standings') }}
    union all
    select 'team_adv_stats', count(*), max(__created_at) from {{ ref('team_adv_stats') }}
    union all
    select 'team_blown_leads', count(*), max(__created_at) from {{ ref('team_blown_leads') }}
    union all
    select 'team_contracts_analysis', count(*), max(__created_at) from {{ ref('team_contracts_analysis') }}
    union all
    select 'team_odds_outcomes', count(*), max(__created_at) from {{ ref('team_odds_outcomes') }}
    union all
    select 'team_ratings', count(*), max(__created_at) from {{ ref('team_ratings') }}
    union all
    select 'team_record_daily_rollup', count(*), max(__created_at) from {{ ref('team_record_daily_rollup') }}
    union all
    select 'transactions', count(*), max(__created_at) from {{ ref('transactions') }}
    union all
    select 'schedule_tonights_games', count(*), max(__created_at) from {{ ref('schedule_tonights_games') }}
),

final as (
    select
        dashboard_gold_tables.gold_table,
        check_context.check_date,
        check_context.checked_at,
        coalesce(gold_source_health.mapped_source_count, 0) as mapped_source_count,
        coalesce(gold_source_health.fresh_source_count, 0) as fresh_source_count,
        coalesce(gold_source_health.source_records_today, 0) as source_records_today,
        gold_source_health.latest_source_activity_at,
        gold_source_health.source_flags,
        gold_source_health.bronze_source_tables,
        coalesce(gold_source_health.is_source_fresh, false) as is_source_fresh,
        gold_table_stats.gold_row_count,
        gold_table_stats.gold_refreshed_at,
        coalesce(gold_table_stats.gold_refreshed_at::date = check_context.check_date, false) as is_gold_refreshed,
        coalesce(gold_source_health.is_source_fresh, false)
            and coalesce(gold_table_stats.gold_refreshed_at::date = check_context.check_date, false) as is_pipeline_healthy
    from dashboard_gold_tables
        cross join check_context
        left join gold_source_health
            on dashboard_gold_tables.gold_table = gold_source_health.gold_table
        left join gold_table_stats
            on dashboard_gold_tables.gold_table = gold_table_stats.gold_table
)

select *
from final
order by gold_table
