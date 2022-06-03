{{ config(materialized='incremental') }}

-- dbt run --full-refresh --select staging_aws_boxscores_incremental_table
with my_cte as (
    select distinct
        {{ clean_player_names_bbref('player') }}::text as player,
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
        date::date,
        case when date < '2022-04-11' then 'Regular Season' when date > '2022-04-11' and date < '2022-04-16' then 'Play-In' else 'Playoffs' end as type,
        season
    from {{ source('nba_source', 'aws_boxscores_source')}}
    where player is not null
    order by date desc
)

select *
from my_cte

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- only grab records where date is greater than the max date of the existing records in the tablegm
  where date > (select max(date) from {{ this }})

{% endif %}