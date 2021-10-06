with player_salary as (
    select player,
           salary
    from {{ ref('staging_aws_contracts_table')}}
),

final_aws_gamelogs as (
    SELECT
           g.player,
           g.team,
           g.location,
           g.opponent,
           g.outcome,
           g.mp,
           g.fgm,
           g.fga,
           g.fgpercent,
           g.threepfgmade,
           g.threepattempted,
           g.threepointpercent,
           g.ft,
           g.fta,
           g.ftpercent,
           g.oreb,
           g.dreb,
           g.trb,
           g.ast,
           g.stl,
           g.blk,
           g.tov,
           g.pf,
           g.pts,
           g.plusminus,
           g.gmsc,
           g.date,
           g.type,
           g.season,
           g.game_ts_percent,
           g.season_ts_percent,
           g.season_avg_ppg,
           g.games_played,
           c.salary
    FROM {{ ref('staging_aws_boxscores_table')}} g
    LEFT JOIN player_salary c using (player)

)

SELECT * 
FROM final_aws_gamelogs