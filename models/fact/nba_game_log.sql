{{ config(materialized='incremental') }}

with boxscores_enriched as (
    select
        player,
        team,
        date as game_date,
        location,
        opponent,
        outcome,
        mp,
        fgm,
        fga,
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
        pts,
        coalesce(plusminus, 0) as plus_minus,
        gmsc,
        season_type,
        season,
        {{ generate_ts_percent('pts', 'fga', 'fta::numeric') }} as game_ts_percent,
        round((
            pts::numeric + (0.5 * plusminus::numeric) + (2 * (stl::numeric + blk::numeric))
            + (0.5 * trb::numeric) - (1.5 * tov::numeric) + (1.5 * ast::numeric)
        ), 1)::numeric as game_mvp_score,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_boxscores_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        and modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from boxscores_enriched
