{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

with schedule_data as (
    select
        {{ dbt_utils.generate_surrogate_key(["home_team::text", "proper_date::date"]) }} as id,
        away_team::text as away_team,
        away_team_attributes.team_acronym as away_team_acronym,
        home_team::text as home_team,
        home_team_attributes.team_acronym as home_team_acronym,
        date::text as date,
        proper_date::date as proper_date,
        substr(start_time, 0, length(start_time) - 0)::text as start_time,
        to_char(proper_date, 'Day') as day_name,
        bbref_league_schedule.created_at,
        bbref_league_schedule.modified_at
    from {{ source('nba_source', 'bbref_league_schedule') }}
        left join {{ source('nba_source', 'internal_team_attributes') }} as home_team_attributes
            on bbref_league_schedule.home_team = home_team_attributes.team
        left join {{ source('nba_source', 'internal_team_attributes') }} as away_team_attributes
            on bbref_league_schedule.away_team = away_team_attributes.team
    where
        -- in the playoffs bbref creates all potential playoff game records even if the series never games to
        -- games 5, 6, or 7.  if these games never get played, they have an empty start_time
        start_time != ''
        and start_time != '11:00' -- historical bug, i think invalid games were being given start times of 11:00
    {% if is_incremental() %}
            and bbref_league_schedule.modified_at > (select max(modified_at) from {{ this }})

        {% endif %}
)

select *
from schedule_data
