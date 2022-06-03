with player_most_recent_date as (
    select
        player,
        max(date)::date as date
    from {{ ref('staging_aws_boxscores_incremental_table') }}
    group by player
),

player_teams as (
    select
        b.player,
        b.team,
        b.date
    from {{ ref('staging_aws_boxscores_incremental_table') }} b
    inner join player_most_recent_date using (player, date)

),

final as (
    select distinct
        player,
        team
    from player_teams
    where date is not null
)

select *
from final