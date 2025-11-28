with current_rosters as (
    select
        player,
        team
    from {{ ref('dim_players_team_history_scd2') }}
    where is_current_team = 1
),

player_stats as (
    select
        player,
        coalesce(vorp, 0) as clean_vorp
    from {{ ref('dim_players') }}
),

active_injuries as (
    select
        player,
        is_player_out -- Assumes 1 = Out, 0 = Active/Day-to-Day
    from {{ ref('fact_injury_data') }}
),

joined_data as (
    select
        current_rosters.team,
        current_rosters.player,
        player_stats.clean_vorp as total_vorp,

        -- Handle NULLs from the left join (NULL means no injury record found = Healthy)
        coalesce(active_injuries.is_player_out, 0) as is_player_out,

        -- Simplified Logic
        case
            -- If player is OUT (1), Available is 0. Otherwise, it's their full VORP.
            when coalesce(active_injuries.is_player_out, 0) = 1 then 0
            else player_stats.clean_vorp
        end as available_vorp,

        case
            -- If player is OUT (1), Missing is their VORP. Otherwise, 0.
            when coalesce(active_injuries.is_player_out, 0) = 1 then player_stats.clean_vorp
            else 0
        end as missing_vorp

    from current_rosters
        left join player_stats
            on current_rosters.player = player_stats.player
        left join active_injuries
            on current_rosters.player = active_injuries.player
)

select * from joined_data
