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
    from {{ source('nba_source', 'aws_team_attributes_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from teams
