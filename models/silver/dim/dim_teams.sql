{{ config(materialized='incremental') }}

with teams as (
    select
        team::text as team,
        team_acronym::text as team_acronym,
        conference,
        primary_color,
        secondary_color,
        third_color,
        previous_season_wins,
        previous_season_rank,
        team_logo,
        created_at,
        modified_at
    from {{ source('bronze', 'internal_team_attributes') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select coalesce(max(modified_at), '1900-01-01'::timestamp) from {{ this }})

    {% endif %}
)

select *
from teams
