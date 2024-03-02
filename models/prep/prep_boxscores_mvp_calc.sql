with boxscores as (
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
        boxscores.player,
        boxscores.team,
        boxscores.date as game_date,
        game_ids.game_id,
        boxscores.location,
        boxscores.opponent,
        boxscores.outcome,
        boxscores.mp,
        boxscores.fgm,
        boxscores.fga,
        boxscores.fgpercent,
        boxscores.threepfgmade,
        boxscores.threepattempted,
        boxscores.threepointpercent,
        boxscores.ft,
        boxscores.fta,
        boxscores.ftpercent,
        boxscores.oreb,
        boxscores.dreb,
        boxscores.trb,
        boxscores.ast,
        boxscores.stl,
        boxscores.blk,
        boxscores.tov,
        boxscores.pf,
        boxscores.pts,
        coalesce(boxscores.plusminus, 0) as plus_minus,
        boxscores.gmsc,
        boxscores.season_type,
        boxscores.season,
        {{ generate_ts_percent('boxscores.pts', 'boxscores.fga', 'boxscores.fta::numeric') }} as game_ts_percent,
        round((
            pts::numeric + (0.5 * plusminus::numeric) + (2 * (stl::numeric + blk::numeric))
            + (0.5 * trb::numeric) - (1.5 * tov::numeric) + (1.5 * ast::numeric)
        ), 1)::numeric as game_mvp_score
    from boxscores
        left join game_ids
            on
                boxscores.team = game_ids.team
                and boxscores.date = game_ids.date
                and boxscores.opponent = game_ids.opponent
)

select *
from final
