with player_salary as (
    select
        player,
        salary
    from {{ ref('staging_aws_contracts_table')}}
),

final_aws_gamelogs as (
    select distinct
        staging_aws_boxscores_table.player,
        staging_aws_boxscores_table.team,
        staging_aws_boxscores_table.location,
        staging_aws_boxscores_table.opponent,
        staging_aws_boxscores_table.outcome,
        staging_aws_boxscores_table.mp,
        staging_aws_boxscores_table.fgm,
        staging_aws_boxscores_table.fga,
        staging_aws_boxscores_table.fgpercent,
        staging_aws_boxscores_table.threepfgmade,
        staging_aws_boxscores_table.threepattempted,
        staging_aws_boxscores_table.threepointpercent,
        staging_aws_boxscores_table.ft,
        staging_aws_boxscores_table.fta,
        staging_aws_boxscores_table.ftpercent,
        staging_aws_boxscores_table.oreb,
        staging_aws_boxscores_table.dreb,
        staging_aws_boxscores_table.trb,
        staging_aws_boxscores_table.ast,
        staging_aws_boxscores_table.stl,
        staging_aws_boxscores_table.blk,
        staging_aws_boxscores_table.tov,
        staging_aws_boxscores_table.pf,
        staging_aws_boxscores_table.pts,
        staging_aws_boxscores_table.plusminus,
        staging_aws_boxscores_table.gmsc,
        staging_aws_boxscores_table.date,
        staging_aws_boxscores_table.type,
        staging_aws_boxscores_table.season,
        staging_aws_boxscores_table.game_ts_percent,
        staging_aws_boxscores_table.season_ts_percent,
        staging_aws_boxscores_table.season_avg_ppg,
        staging_aws_boxscores_table.games_played,
        player_salary.salary
    from {{ ref('staging_aws_boxscores_table')}}
    left join player_salary using (player)

)

select *
from final_aws_gamelogs
