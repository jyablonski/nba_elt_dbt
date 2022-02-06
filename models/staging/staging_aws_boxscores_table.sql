with my_cte as (
    select 
        distinct *
    from {{ source('nba_source', 'aws_boxscores_source')}} /* gamelogs got like 12x counted on 12-13-21 for some reason */
),

season_stats as (
    SELECT 
            player::text as player,
            sum(fga::numeric) as fga_total,
            sum(fta::numeric) as fta_total,
            sum(pts::numeric) as pts_total,
            sum(plusminus::numeric) as plusminus_total,
            COUNT(*) as games_played,
    type::text as type
    FROM my_cte
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
           coalesce(plusminus, 0) as plusminus,
           gmsc,
           date,
           type,
           season
    FROM my_cte
    WHERE player IS NOT NULL

),

game_ids as (
    SELECT distinct
     DENSE_RANK() OVER (
         ORDER BY 
              date,(
                CASE
                    WHEN team < opponent THEN CONCAT(team,opponent)
                    ELSE CONCAT(opponent,team)
                END
              )
     ) as game_id,     
     team,
     date,
     opponent
     FROM my_cte
    
),

final_aws_boxscores as (
    SELECT g.player,
           g.team,
           i.game_id,
           g.date,
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
           coalesce(g.plusminus, 0) as plusminus,
           g.gmsc,
           g.type,
           g.season,
           {{ generate_ts_percent('g.pts', 'g.fga', 'g.fta::numeric') }} as game_ts_percent,
           {{ generate_ts_percent('s.pts_total', 's.fga_total', 's.fta_total::numeric') }} as season_ts_percent,
           round(s.pts_total / s.games_played, 1)::numeric as season_avg_ppg,
           round(s.plusminus_total / s.games_played, 1)::numeric as season_avg_plusminus,
           s.games_played as games_played
    from game_stats g
    LEFT JOIN season_stats s using (player)
    LEFT JOIN game_ids i using (team, date, opponent)

),

mvp_calc as (
    select
        player,
        type,
        round(
            avg(
                     pts::numeric
            ) + (
                     0.5 * avg(plusminus::numeric)
            ) + (
                2 * avg(stl::numeric + blk::numeric)
            ) + (
                0.5 * avg(trb::numeric)
            ) + (1.5 * avg(ast::numeric)) - (1.5 * avg(tov::numeric)),
            1
        ) as player_mvp_calc_avg
    from final_aws_boxscores
    group by player, type

),

final as (
    select b.player,
           b.team,
           b.game_id,
           b.date,
           b.location,
           b.opponent,
           b.outcome,
           b.mp,
           b.fgm,
           b.fga,
           b.fgpercent,
           b.threepfgmade,
           b.threepattempted,
           b.threepointpercent,
           b.ft,
           b.fta,
           b.ftpercent,
           b.oreb,
           b.dreb,
           b.trb,
           b.ast,
           b.stl,
           b.blk,
           b.tov,
           b.pf,
           b.pts,
           b.plusminus,
           b.gmsc,
           b.type,
           b.season,
           b.game_ts_percent,
           b.season_ts_percent,
           b.season_avg_ppg,
           b.season_avg_plusminus,
           b.games_played,
           m.player_mvp_calc_avg,
           a.team as full_team,
        round((pts::numeric + (0.5 * plusminus::numeric) + (2 * (stl::numeric + blk::numeric)) +
        (0.5 * trb::numeric) - (1.5 * tov::numeric) + (1.5 * ast::numeric)), 1)::numeric as player_mvp_calc_game
    from final_aws_boxscores b
    left join mvp_calc m on m.player = b.player and m.type = b.type
    left join {{ ref('staging_seed_team_attributes')}} a on a.team_acronym = b.team
)

SELECT 
    *
FROM final