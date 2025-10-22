{{ config(materialized='incremental') }}

with odds_cte as (
    select
        ltrim(team)::text as team,
        spread::numeric as spread,
        total::numeric as total,
        coalesce(moneyline, 100)::numeric as moneyline,
        datetime1::timestamp as time,
        date::date as date,
        replace(team, ' ', '') as team_acronym,
        created_at,
        modified_at
    from {{ source('nba_source', 'draftkings_game_odds') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where created_at > (select coalesce(max(created_at), '1900-01-01'::timestamp) from {{ this }})

    {% endif %}

)

select
    team,
    {{ convert_team_names('team_acronym') }} as team_acronym,
    spread,
    total,
    moneyline,
    time,
    date,
    created_at,
    modified_at
from odds_cte
