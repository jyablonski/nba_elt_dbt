{{ config(materialized='incremental') }}

with my_cte as (
    SELECT 
        player,
        MD5(player::text) as md5_player,              /* ::text works here */
        SHA256(player::bytea)::text as sha256_player,
        SHA512(player::bytea)::text as sha512_player, /* bytea needed; ::text wont work */
        {{ dbt_utils.hash('player') }}as player_hash, /* this is just MD5, so it's the same as md5_player */
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
  -- only grab records where date is greater than the max date of the existing records in the tablegm
  where date > (select max(date) from {{ this }})

{% endif %}