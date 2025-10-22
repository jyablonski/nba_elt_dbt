{{ config(materialized='incremental') }}

with preseason_odds as (
    select
        bbref_team_preseason_odds.team::text,
        internal_team_attributes.team_acronym,
        bbref_team_preseason_odds.odds::numeric as championship_odds,
        bbref_team_preseason_odds.predicted::numeric as predicted_wins,
        82 - bbref_team_preseason_odds.predicted as predicted_losses,
        bbref_team_preseason_odds.created_at,
        bbref_team_preseason_odds.modified_at
    from {{ source('nba_source', 'bbref_team_preseason_odds') }}
        left join {{ source('nba_source', 'internal_team_attributes') }}
            on
                bbref_team_preseason_odds.team = internal_team_attributes.team
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where bbref_team_preseason_odds.modified_at > (select coalesce(max(modified_at), '1900-01-01'::timestamp) from {{ this }})

    {% endif %}
)

select *
from preseason_odds
