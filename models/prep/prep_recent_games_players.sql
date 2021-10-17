with player_season_high as (
    SELECT  player,
            max(pts) as max_pts,
            max(game_ts_percent) as max_ts
    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY player
),

boxscores_yesterday as (
    SELECT max(date) as date
    FROM {{ ref('staging_aws_boxscores_table')}}
),

player_contracts as (
    SELECT player,
            salary
    FROM {{ ref('staging_aws_contracts_table')}}
),

player_logo as (
    SELECT player,
            headshot as player_logo
    FROM {{ ref('staging_seed_player_attributes')}}
),

final_table as (
    SELECT *,
            CASE WHEN pts = max_pts THEN 1
            ELSE 0
            END AS pts_color,
            CASE WHEN game_ts_percent = max_ts THEN 1
            ELSE 0
            END AS ts_color,
            CONCAT('<span style=''font-size:16px; color:royalblue;''>', player,'</span> <span style=''font-size:12px; color:grey;''>', team, '</span>') as player_new
    FROM {{ ref('staging_aws_boxscores_table')}}
    INNER JOIN boxscores_yesterday using (date)
    LEFT JOIN player_season_high using (player)
    LEFT JOIN player_contracts using (player)
    LEFT JOIN player_logo using (player)
    ORDER BY pts DESC
)

SELECT *
FROM final_table
