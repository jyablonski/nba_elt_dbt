{{ config(materialized='incremental') }}

with boxscores_enriched as (
    select
        {{ clean_player_names_bbref('player') }}::text as player,
        team,
        date::date as game_date,
        location,
        opponent,
        outcome,
        mp,
        fgm::numeric,
        fga::numeric,
        fgpercent::numeric,
        threepfgmade::numeric,
        threepattempted::numeric,
        threepointpercent::numeric,
        ft::numeric,
        fta::numeric,
        ftpercent,
        oreb::numeric,
        dreb::numeric,
        trb::numeric,
        ast::numeric,
        stl::numeric,
        blk::numeric,
        tov::numeric,
        pf::numeric,
        pts::numeric,
        coalesce(plusminus, 0) as plus_minus,
        gmsc::numeric,
        {{ generate_season_type('date') }}::text as season_type,
        {{ generate_ts_percent('pts::numeric', 'fga::numeric', 'fta::numeric') }} as game_ts_percent,
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
        where modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from boxscores_enriched
