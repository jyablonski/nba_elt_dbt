/*
table has player agg stats + season mvp calc for top 20 pt scorer table and contract value analysis
my idea for int tables it to have views for these intermediate steps between staging and prod to take a look at how things are being calculated.

TO DO
1) to get most recent team ima have to like grab every player's most recent game played in like 3 ctes

2) create missed games variable

3) need to make adjusted mvp calc by accounting for missed games, given % of games throuihout a season

4) need to create case when for superstar / great / average / bad value.
*/

{{ config(enabled = false) }}

with total_player_stats as (
    select
        player,
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
        count(distinct(game_id))::integer as tot_games_played

    from {{ ref('staging_aws_boxscores_table')}}
    group by player, type
),

/* this cte shouuuuld be grabbing the most recent game each player has played, and using THAT team as their active team
if you get traded while injured then fkn rip */

player_most_recent_date as (
    select
        player,
        max(date) as most_recent_gp
    from {{ ref('staging_aws_boxscores_table')}}
    group by player
),

player_teams as (
    select
        staging_aws_boxscores_table.player,
        staging_aws_boxscores_table.team,
        player_most_recent_date.most_recent_gp
    from {{ ref('staging_aws_boxscores_table')}}
    left join player_most_recent_date using (player)

),

team_games as (
    select
        team,
        count(distinct(game_id))::integer as tot_team_games_played
    from {{ ref('staging_aws_boxscores_table')}}
    group by team
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
        ) as player_mvp_calc
    from {{ ref('staging_aws_boxscores_table')}}
    group by player, type

),

contract_df as (
    select
        player,
        salary
    from {{ ref('staging_aws_contracts_table')}}
),

combined_table as (
    select
        distinct total_player_stats.player,
        player_teams.team,
        total_player_stats.type,
        total_player_stats.tot_games_played,
        team_games.tot_team_games_played,
        mvp_calc.player_mvp_calc,
        contract_df.salary::numeric,
        tot_team_games_played - tot_games_played as games_missed

    from total_player_stats
    left join
        mvp_calc on
            mvp_calc.player = total_player_stats.player and mvp_calc.type = total_player_stats.type
    left join contract_df on total_player_stats.player = contract_df.player
    left join player_teams on total_player_stats.player = player_teams.player
    left join team_games on player_teams.team = team_games.team
)

select * from combined_table
