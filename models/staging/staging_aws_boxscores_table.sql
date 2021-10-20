with season_stats as (
    select
         player::text as player,
         type::text as type,
         sum(fga::numeric) as fga_total,
         sum(fta::numeric) as fta_total,
         sum(pts::numeric) as pts_total,
        count(*) as games_played
    from {{ source('nba_source', 'aws_boxscores_source')}}
    where player is not null
    group by player, type
),

/*      pts / (2 * (fga + (fta::numeric * 0.44))) as hm */
game_stats as (
    select
         player,
        team,
        location,
        opponent,
        outcome,
        mp,
        fgm,
        fga::numeric,
        fgpercent,
        threepfgmade,
        threepattempted,
        threepointpercent,
        ft,
        fta,
        ftpercent,
        oreb,
        dreb,
        trb,
        ast,
        stl,
        blk,
        tov,
        pf,
        pts::numeric,
        plusminus,
        gmsc,
        date,
        type,
        season
    from {{ source('nba_source', 'aws_boxscores_source')}}
    where player is not null

),

game_ids as (
    select distinct
        team,
        date,
        opponent,
        dense_rank() over (
            order by
                date, (
                    case
                         when team < opponent then concat(team, opponent)
                         else concat(opponent, team)
                     end
                )
        ) as game_id
    from {{ source('nba_source', 'aws_boxscores_source')}}

),

final_aws_boxscores as (
    select
         game_stats.player,
        game_stats.team,
        game_ids.game_id,
        game_stats.date,
        game_stats.location,
        game_stats.opponent,
        game_stats.outcome,
        game_stats.mp,
        game_stats.fgm,
        game_stats.fga,
        game_stats.fgpercent,
        game_stats.threepfgmade,
        game_stats.threepattempted,
        game_stats.threepointpercent,
        game_stats.ft,
        game_stats.fta,
        game_stats.ftpercent,
        game_stats.oreb,
        game_stats.dreb,
        game_stats.trb,
        game_stats.ast,
        game_stats.stl,
        game_stats.blk,
        game_stats.tov,
        game_stats.pf,
        game_stats.pts,
        game_stats.plusminus,
        game_stats.gmsc,
        game_stats.type,
        game_stats.season,
           {{ generate_ts_percent('g.pts', 'g.fga', 'g.fta::numeric') }} as game_ts_percent,
           {{ generate_ts_percent('s.pts_total', 's.fga_total', 's.fta_total::numeric') }} as season_ts_percent,
        case
            when
                season_stats.pts_total = 0 and season_stats.fga_total = 0 and season_stats.fta_total::numeric = 0 then null
            else
                round(
                    season_stats.pts_total / (
                        2 * (
                            season_stats.fga_total + (
                                season_stats.fta_total::numeric * 0.44
                            )
                        )
                    ),
                    3
                )
 end as season_ts_percent,
        round(
            season_stats.pts_total / season_stats.games_played, 1
        )::numeric as season_avg_ppg
    from game_stats
    left join season_stats using (player)
    left join game_ids using (team, date, opponent)

)

select * from final_aws_boxscores
