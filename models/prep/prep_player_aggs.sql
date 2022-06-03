with my_cte as (
    select 
        *
    from {{ ref('staging_aws_boxscores_incremental_table') }}
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
    from my_cte
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
    from my_cte
    where type = 'Playoffs'
    group by player, type

),

player_teams as (
    select 
        player,
        team
    from {{ ref('prep_player_most_recent_team') }}
),

final as (
    select
        s.player, 
        team,
        s.fga_total,
        s.fta_total,
        s.pts_total,
        s.plusminus_total,
        s.games_played,
        round(s.pts_total / s.games_played, 1)::numeric as season_avg_ppg,
        {{ generate_ts_percent('s.pts_total', 's.fga_total', 's.fta_total::numeric') }} as season_ts_percent,
        round(s.plusminus_total / s.games_played, 1)::numeric as season_avg_plusminus,
        m.player_mvp_calc_avg,
        mp.player_mvp_calc_avg_playoffs,
        sp.fga_total_playoffs,
        sp.fta_total_playoffs,
        sp.pts_total_playoffs,
        sp.plusminus_total_playoffs,
        sp.games_played_playoffs,
        {{ generate_ts_percent('sp.pts_total_playoffs', 'sp.fga_total_playoffs', 'sp.fta_total_playoffs::numeric') }} as playoffs_ts_percent,
        round(sp.pts_total_playoffs / sp.games_played_playoffs, 1)::numeric as playoffs_avg_ppg,
        round(sp.plusminus_total_playoffs / sp.games_played_playoffs, 1)::numeric as playoffs_avg_plusminus
    from season_stats s
    left join mvp_calc m using (player)
    left join mvp_calc_playoffs mp using (player)
    left join season_stats_playoffs sp using (player)
    left join player_teams using (player)
    order by player_mvp_calc_avg desc
),

final2 as (
    select 
        *,
        row_number() over (order by season_avg_ppg desc) as ppg_rank,
        case when row_number() over (order by season_avg_ppg desc) <= 20
             then 'Top 20 Scorers'
             else 'Other' end as top20_scorers
    from final
)

/*
{{ generate_ts_percent('s.pts_total', 's.fga_total', 's.fta_total::numeric') }} as season_ts_percent,
{{ generate_ts_percent('p.pts_total_playoffs', 'p.fga_total_playoffs', 'p.fta_total_playoffs::numeric') }} as playoffs_ts_percent,
round(s.pts_total / s.games_played, 1)::numeric as season_avg_ppg,
round(p.pts_total_playoffs / p.games_played_playoffs, 1)::numeric as playoffs_avg_ppg,
round(s.plusminus_total / s.games_played, 1)::numeric as season_avg_plusminus,
round(p.plusminus_total_playoffs / p.games_played_playoffs, 1)::numeric as playoffs_avg_plusminus,
*/

select 
    *
from final2