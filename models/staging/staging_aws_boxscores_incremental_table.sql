{{ config(materialized='incremental') }}

with my_cte as (
    SELECT 
        player,
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
    FROM {{ source('nba_source', 'aws_boxscores_source')}}
    WHERE player IS NOT NULL
    order by date desc
)

select *
from my_cte

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date > (select max(date) from {{ this }})

{% endif %}