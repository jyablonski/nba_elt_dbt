with season_stats as (
    SELECT 
            player::text as player,
            sum(fga::numeric) as fga_total,
            sum(fta::numeric) as fta_total,
            sum(pts::numeric) as pts_total,
            COUNT(*) as games_played,
    type::text as type
    FROM {{ source('nba_source', 'aws_boxscores_source')}}
    WHERE player IS NOT NULL
    group by player, type
),

/*      pts / (2 * (fga + (fta::numeric * 0.44))) as hm */
game_stats as (
    SELECT player,
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
    FROM {{ source('nba_source', 'aws_boxscores_source')}}
    WHERE player IS NOT NULL

),

final_aws_boxscores as (
    SELECT g.player,
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
           {{ generate_ts_percent('g.pts', 'g.fga', 'g.fta::numeric') }} as game_ts_percent,
           {{ generate_ts_percent('s.pts_total', 's.fga_total', 's.fta_total::numeric') }} as season_ts_percent,
           round(s.pts_total / s.games_played, 1)::numeric as season_avg_ppg,
           s.games_played as games_played
    from game_stats g
    LEFT JOIN season_stats s using (player)

)

SELECT * FROM final_aws_boxscores