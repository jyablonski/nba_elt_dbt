{{ config(materialized='incremental') }}

with schedule_data as (
    select distinct
        away_team::text as away_team,
        away_team_attributes.team_acronym as away_team_acronym,
        home_team::text as home_team,
        home_team_attributes.team_acronym as home_team_acronym,
        date::text as date,
        proper_date::date as proper_date,
        substr(start_time, 0, length(start_time) - 0)::text as start_time,
        to_char(proper_date, 'Day') as day_name,
        aws_schedule_source.created_at,
        aws_schedule_source.modified_at
    from {{ source('nba_source', 'aws_schedule_source') }}
        left join {{ source('nba_source', 'aws_team_attributes_source') }} as home_team_attributes
            on aws_schedule_source.home_team = home_team_attributes.team
        left join {{ source('nba_source', 'aws_team_attributes_source') }} as away_team_attributes
            on aws_schedule_source.away_team = away_team_attributes.team
    where 
        start_time like '%:%' -- hack to only get records that have a start time (7:00)
        and start_time != '11:00' -- bug, thx bbref
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        and aws_schedule_source.modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from schedule_data
