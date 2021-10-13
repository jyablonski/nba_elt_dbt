/*  
table has player agg stats + season mvp calc for top 20 pt scorer table and contract value analysis
my idea for int tables it to have views for these intermediate steps between staging and prod to take a look at how things are being calculated.

TO DO
1) to get most recent team ima have to like grab every player's most recent game played in like 3 ctes

2) create missed games variable

3) need to make adjusted mvp calc by accounting for missed games, given % of games throuihout a season

4) need to create case when for superstar / great / average / bad value.
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
            sum(plusminus) as tot_plusminus,
            COUNT(DISTINCT(game_id))::integer as tot_games_played

    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY player, type
),

/* this cte shouuuuld be grabbing the most recent game each player has played, and using THAT team as their active team
if you get traded while injured then fkn rip */

player_most_recent_date as (
    SELECT player,
            max(date) as most_recent_gp
    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY player
),

player_teams as (
  SELECT p.player,
      p.team,
      d.most_recent_gp
  FROM {{ ref('staging_aws_boxscores_table')}} p
  LEFT JOIN player_most_recent_date d using (player)

),

team_games as (
    SELECT team,
            COUNT(DISTINCT(game_id))::integer as tot_team_games_played
    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY team
),

mvp_calc as (
    SELECT  player,
            type,
            round(avg(pts::numeric) + (0.5 * avg(plusminus::numeric)) + (2 * avg(stl::numeric + blk::numeric)) + (0.5 * avg(trb::numeric)) + (1.5 * avg(ast::numeric)) - (1.5 * avg(tov::numeric)), 1) as player_mvp_calc
    FROM {{ ref('staging_aws_boxscores_table')}}
    GROUP BY player, type

),

contract_df as (
    SELECT  player,
            salary
    FROM {{ ref('staging_aws_contracts_table')}}
),

combined_table as (
    SELECT  s.player,
            t.team,
            s.type,
            s.tot_games_played,
            tg.tot_team_games_played,
            tot_team_games_played - tot_games_played as games_missed,
            m.player_mvp_calc,
            c.salary::numeric
            
    FROM total_player_stats s
    LEFT JOIN mvp_calc m ON m.player = s.player AND m.type = s.type
    LEFT JOIN contract_df c ON s.player = c.player
    LEFT JOIN player_teams t ON s.player = t.player
    LEFT JOIN team_games tg ON t.team = tg.team
)

SELECT * FROM combined_table
