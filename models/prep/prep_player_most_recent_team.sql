with player_most_recent_date as (
    select
        player,
        max(date)::date as date
    from {{ ref('staging_aws_boxscores_table')}}
    group by player
),

player_teams as (
    select
        staging_aws_boxscores_table.player,
        staging_aws_boxscores_table.team,
        player_most_recent_date.date
    from {{ ref('staging_aws_boxscores_table')}}
    left join player_most_recent_date using (player, date)

),

final as (
    select *
    from player_teams
    where date IS NOT NULL
)

select 
    player,
    team
from final