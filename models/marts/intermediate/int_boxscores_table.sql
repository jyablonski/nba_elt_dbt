/*  
table has player agg stats + season mvp calc for top 20 pt scorer table and contract value analysis
my idea for int tables it to have views for these intermediate steps between staging and prod to take a look at how things are being calculated.
*/

with total_player_stats as (
    SELECT  player,
            type,
            sum(fgm) as tot_fgm,
            sum(fga) as tot_fga,
            sum(threepfgmade) as tot_threepfgmade,
            sum(threepattempted) as tot_threepattempted,
            sum(ft::numeric) as tot_ft,
            sum(fta::numeric) as tot_fta,
            sum(trb) as tot_trb,
            sum(ast) as tot_ast,
            sum(stl) as tot_stl,
            sum(blk) as tot_blk,
            sum(tov) as tot_tov,
            sum(pts) as tot_pts,
            sum(plusminus) as tot_plusminus

    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY player, type
),

player_mvp_calc as (
    SELECT  player,
            type,
            round(avg(pts::numeric) + (0.5 * avg(plusminus::numeric)) + (2 * avg(stl::numeric + blk::numeric)) + (0.5 * avg(trb::numeric)) + (1.5 * avg(ast::numeric)) - (1.5 * avg(tov::numeric)), 1) as player_mvp_calc
    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY player, type

)

SELECT * FROM player_mvp_calc
