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
        coalesce(season_salary, {{ var('default_player_salary') }})::numeric as salary
    from {{ source('bronze', 'bbref_player_contracts') }}
),

player_current_team as (
    select
        player,
        team as contract_team
    from {{ ref('int_player_most_recent_team') }}
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
    coalesce(contracts.salary, {{ var('default_player_salary') }}) as salary,
    player_current_team.contract_team,
    coalesce(is_top_players.rank, 0) as rank,
    adv_stats.pos,
    adv_stats.vorp,
    adv_stats.per,
    adv_stats.bpm,
    coalesce(adv_stats.ws, 0) as win_shares,
    adv_stats.ws_per_48,
    players.created_at,
    players.modified_at
from players
    left join contracts on players.player = contracts.player
    left join player_current_team on players.player = player_current_team.player
    left join is_top_players on players.player = is_top_players.player
    left join adv_stats on players.player = adv_stats.player
