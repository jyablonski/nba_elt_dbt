-- grab season high from ANY game that season
with player_season_high as (
    select
        player,
        max(pts) as max_pts,
        max(game_ts_percent) as max_ts
    from {{ ref('boxscores') }}
    -- where type = 'Regular Season'
    group by player
),

-- yesterday could mean literally yesterday, but just grab the most recent games.
boxscores_yesterday as (
    select max(game_date) as game_date
    from {{ ref('boxscores') }}
),

-- grab (most recent) team from above cte
boxscores_cte as (
    select *
    from {{ ref('boxscores') }}
),

player_aggs as (
    select distinct
        prep_player_stats.player,
        avg_ppg,
        salary,
        headshot as player_logo
    from {{ ref('prep_player_stats') }}
        left join {{ ref('players') }} using (player)
    where season_type = 'Regular Season'
),

final_table as (
    select distinct
        *,
        case
            when pts = max_pts then 1
            when (pts >= avg_ppg + 10) and (pts != max_pts) then 2
            when avg_ppg - pts > 10 then 3
            else 0
        end as pts_color,
        case
            when game_ts_percent = max_ts then 1
            else 0
        end as ts_color,
        concat(
            '<span style=''font-size:16px; color:royalblue;''>', player, '</span> <span style=''font-size:12px; color:grey;''>', team, '</span>'
        ) as player_new
    from boxscores_cte
        inner join boxscores_yesterday using (game_date)
        left join player_season_high using (player)
        left join player_aggs using (player)
    order by pts desc
)

select *
from final_table
