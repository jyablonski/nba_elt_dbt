{{
    config(
        materialized='table',
        tags=['GOLD', 'OPS', 'DAILY'],
        post_hook=['{{ append_ingestion_freshness_history() }}']
    )
}}

/*
Daily ops mart: for each enabled ingestion feature flag with a bronze write,
verify the mapped bronze source received rows created or modified on today's
Pacific date. Post_hook upserts history: one row per flag/check_date when
records_today > 0; replaces prior rows only when records_today was null/zero.
*/

with check_context as (
    select
        ({{ dbt.current_timestamp() }} at time zone '{{ var("dbt_date:time_zone") }}')::date as check_date,
        {{ dbt.current_timestamp() }} as checked_at
),

enabled_flags as (
    select
        id,
        flag,
        is_enabled,
        created_at,
        modified_at
    from {{ source('gold', 'feature_flags') }}
    where is_enabled = 1
),

-- 1:1 with ingestion write_to_sql / write_to_sql_upsert targets in ingestion
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
)

select
    enabled_flags.id as feature_flag_id,
    enabled_flags.flag,
    flag_bronze_map.bronze_source_table,
    flag_bronze_map.write_method,
    check_context.check_date,
    check_context.checked_at,
    coalesce(bronze_today.records_today, 0) as records_today,
    bronze_today.latest_activity_at,
    case
        when coalesce(bronze_today.records_today, 0) > 0 then 'fresh'
        else 'stale'
    end as freshness_status,
    coalesce(bronze_today.records_today, 0) > 0 as is_fresh
from enabled_flags
inner join flag_bronze_map
    on enabled_flags.flag = flag_bronze_map.flag
cross join check_context
left join bronze_today
    on flag_bronze_map.bronze_source_table = bronze_today.bronze_source_table
order by enabled_flags.flag
