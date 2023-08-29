with my_cte as (
    select *
    from {{ ref('staging_aws_boxscores_incremental_table') }}
),

/*      pts / (2 * (fga + (fta::numeric * 0.44))) as hm */

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
    from {{ ref('staging_aws_boxscores_incremental_table') }}

),

final as (
    select
        g.player,
        g.team,
        g.date,
        i.game_id,
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
        round((
            pts::numeric + (0.5 * plusminus::numeric) + (2 * (stl::numeric + blk::numeric))
            + (0.5 * trb::numeric) - (1.5 * tov::numeric) + (1.5 * ast::numeric)
        ), 1)::numeric as player_mvp_calc_game
    from my_cte as g
        left join game_ids as i using (team, date, opponent)

)

select *
from final
