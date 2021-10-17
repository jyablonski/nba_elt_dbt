with player_season_high as (
    select
        player,
        max(pts) as max_pts,
        max(game_ts_percent) as max_ts
    from {{ ref('staging_aws_boxscores_table')}}
    group by player
),

boxscores_yesterday as (
    select max(date) as date
    from {{ ref('staging_aws_boxscores_table')}}
),

player_contracts as (
    select
        player,
        salary
    from {{ ref('staging_aws_contracts_table')}}
),

player_logo as (
    select
        player,
        headshot as player_logo
    from {{ ref('staging_seed_player_attributes')}}
),

final_table as (
    select
        *,
        case when pts = max_pts then 1
            else 0
        end as pts_color,
        case when game_ts_percent = max_ts then 1
            else 0
        end as ts_color,
        concat(
            '<span style=''font-size:16px; color:royalblue;''>', player, '</span> <span style=''font-size:12px; color:grey;''>', team, '</span>'
        ) as player_new
    from {{ ref('staging_aws_boxscores_table')}}
    inner join boxscores_yesterday using (date)
    left join player_season_high using (player)
    left join player_contracts using (player)
    left join player_logo using (player)
    order by pts desc
)

select *
from final_table
