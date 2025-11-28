with player_records as (
    select
        {{ clean_player_names_bbref('player') }}::text as player,
        team,
        date as game_date,
        {{ dbt_utils.generate_surrogate_key(["player", "team"]) }} as scd_id
    from {{ source('bronze', 'bbref_player_boxscores') }}
),

max_dates as (
    select
        player,
        max(game_date) as max_date
    from player_records
    group by player
),

-- idea here is im recalcuating the effective dates for each player
-- whenever they have a new record pop up in the source data
player_team_effective_dates as (
    select
        scd_id,
        player,
        team,
        min(game_date) as valid_from,
        max(game_date) as max_game_date
    from player_records
    group by
        scd_id,
        player,
        team
),

final as (
    select
        scd_id,
        player_team_effective_dates.player,
        player_team_effective_dates.team,
        valid_from,
        case when max_game_date = max_date then '9999-12-31' else max_game_date end as valid_to,
        case when max_game_date = max_date then 1 else 0 end as is_current_team,
        current_timestamp as modified_at
    from player_team_effective_dates
        inner join max_dates on player_team_effective_dates.player = max_dates.player
)

select *
from final
