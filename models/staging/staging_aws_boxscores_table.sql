with my_cte as (
    select 
        distinct
        {{ clean_player_names_bbref('player') }}::text as player,
        team::text as team,
        location::text as location,
        opponent::text as opponent,
        outcome::text as outcome,
        mp::text as mp,
        fgm::integer as fgm,
        fga::integer as fga,
        fgpercent as fgpercent,
        threepfgmade::integer as threepfgmade,
        threepattempted::integer as threepattempted,
        threepointpercent as threepointpercent,
        ft::integer as ft,
        fta::integer as fta,
        ftpercent as ftpercent,
        oreb::integer as oreb,
        dreb::integer as dreb,
        trb::integer as trb,
        ast::integer as ast,
        stl::integer as stl,
        blk::integer as blk,
        tov::integer as tov,
        pf::integer as pf,
        pts::integer as pts,
        plusminus::numeric as plusminus,
        gmsc::numeric as gmsc,
        date::date as date,
        case when date < '2022-04-11' then 'Regular Season' when date > '2022-04-11' and date < '2022-04-16' then 'Play-In' else 'Playoffs' end as type,
        season::text as season
    from {{ source('nba_source', 'aws_boxscores_source')}} /* gamelogs got like 12x counted on 12-13-21 for some reason */
),

season_stats as (
    select 
            player::text as player,
            sum(fga::numeric) as fga_total,
            sum(fta::numeric) as fta_total,
            sum(pts::numeric) as pts_total,
            sum(plusminus::numeric) as plusminus_total,
            COUNT(*) as games_played
    from my_cte
    where player is not null and type = 'Regular Season'
    group by player
),

season_stats_playoffs as (
    select 
            player::text as player,
            sum(fga::numeric) as fga_total_playoffs,
            sum(fta::numeric) as fta_total_playoffs,
            sum(pts::numeric) as pts_total_playoffs,
            sum(plusminus::numeric) as plusminus_total_playoffs,
            COUNT(*) as games_played_playoffs
    from my_cte
    where player is not null and type = 'Playoffs'
    group by player
),

/*      pts / (2 * (fga + (fta::numeric * 0.44))) as hm */
game_stats as (
    select player,
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
    from my_cte
    where player is not null

),

game_ids as (
    select distinct
     DENSE_RANK() over (
         order by 
              date,(
                case
                    when team < opponent then CONCAT(team,opponent)
                    else CONCAT(opponent,team)
                end
              )
     ) as game_id,     
     team,
     date,
     opponent
     from my_cte
    
),

final_aws_boxscores as (
    select g.player,
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
           {{ generate_ts_percent('p.pts_total_playoffs', 'p.fga_total_playoffs', 'p.fta_total_playoffs::numeric') }} as playoffs_ts_percent,
           round(s.pts_total / s.games_played, 1)::numeric as season_avg_ppg,
           round(p.pts_total_playoffs / p.games_played_playoffs, 1)::numeric as playoffs_avg_ppg,
           round(s.plusminus_total / s.games_played, 1)::numeric as season_avg_plusminus,
           round(p.plusminus_total_playoffs / p.games_played_playoffs, 1)::numeric as playoffs_avg_plusminus,
           s.games_played as games_played,
           p.games_played_playoffs as games_played_playoffs
    from game_stats as g
    left join season_stats as s using (player)
    left join season_stats_playoffs as p using (player)
    left join game_ids as i using (team, date, opponent)

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
    where type = 'Regular Season'
    group by player, type

),

mvp_calc_playoffs as (
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
        ) as player_mvp_calc_avg_playoffs
    from final_aws_boxscores
    where type = 'Playoffs'
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
           b.playoffs_ts_percent,
           b.season_avg_ppg,
           b.playoffs_avg_ppg,
           b.season_avg_plusminus,
           b.playoffs_avg_plusminus,
           b.games_played,
           b.games_played_playoffs as playoffs_games_played,
           m.player_mvp_calc_avg,
           p.player_mvp_calc_avg_playoffs,
           a.team as full_team,
        round((pts::numeric + (0.5 * plusminus::numeric) + (2 * (stl::numeric + blk::numeric)) +
        (0.5 * trb::numeric) - (1.5 * tov::numeric) + (1.5 * ast::numeric)), 1)::numeric as player_mvp_calc_game
    from final_aws_boxscores as b
    left join mvp_calc as m on m.player = b.player
    left join mvp_calc_playoffs p on p.player = b.player
    left join {{ ref('staging_seed_team_attributes')}} as a on a.team_acronym = b.team
)

select 
    *
from final
