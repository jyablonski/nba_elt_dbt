-- goal is to recreate `prep/prep_player_most_recent_team.sql`
-- easier way to do with snowflake syntax and partition by / group bys in same cte
-- https://github.com/dbt-labs/dbt-core/issues/3878
{{ config(
    materialized = 'incremental',
    unique_key = 'scd_id',
) }}

with player_records as (
    select
        player,
        team,
        date,
        {{ dbt_utils.generate_surrogate_key(["player", "team"]) }} as scd_id
    from {{ ref('staging_aws_boxscores_incremental_table') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where date > (select max(valid_to) from {{ this }})

    {% endif %}
),

max_dates as (
    select
        player,
        max(date) as max_date
    from player_records
    group by player
),

player_team_effective_dates as (
    select
        scd_id,
        player,
        team,
        count(*) as games_played_for_team,
        min(date) as valid_from,
        max(date) as max_game_date
    from player_records
    group by scd_id, player, team
),

final as (
    select
        scd_id,
        player_team_effective_dates.player,
        player_team_effective_dates.team,
        games_played_for_team,
        valid_from,
        case when max_game_date = max_date then '9999-12-31' else max_game_date end as valid_to,
        case when max_game_date = max_date then 1 else 0 end as is_current_team
    from player_team_effective_dates
        inner join max_dates using (player)
)

select *
from final
