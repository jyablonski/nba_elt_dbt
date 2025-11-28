{{ config(materialized='table') }}

with players as (
    select
        {{ clean_player_names_bbref('player') }}::text as player,
        is_rookie,
        yrs_exp,
        headshot,
        created_at,
        modified_at
    from {{ source('bronze', 'internal_player_attributes') }}
),

contracts as (
    select
        {{ clean_player_names_bbref('player') }}::text as player,
        coalesce(season_salary, 1000000)::numeric as salary
    from {{ source('bronze', 'bbref_player_contracts') }}
),

is_top_players as (
    select
        player,
        team,
        rank
    from {{ source('bronze', 'internal_team_top_players') }}
),

adv_stats as (
    -- DISTINCT ON (player) keeps only the first row it finds for each player
    select distinct on (player)
        player,
        pos,
        vorp,
        per,
        bpm,
        ws,
        "ws/48" as ws_per_48
    from {{ source('bronze', 'bbref_player_adv_stats') }}
    -- We sort by Games (g) DESC so the '2TM' (Total) row is always 1st
    order by
        player asc,
        g desc
)

select distinct
    players.player,
    players.is_rookie,
    players.yrs_exp,
    players.headshot,
    coalesce(contracts.salary, 1000000) as salary,
    coalesce(is_top_players.rank, 0) as rank,
    adv_stats.pos,
    adv_stats.vorp,
    adv_stats.per,
    adv_stats.bpm,
    adv_stats.ws_per_48,
    players.created_at,
    players.modified_at
from players
    left join contracts on players.player = contracts.player
    left join is_top_players on players.player = is_top_players.player
    left join adv_stats on players.player = adv_stats.player
