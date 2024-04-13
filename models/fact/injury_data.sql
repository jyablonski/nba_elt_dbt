{{ config(materialized='incremental') }}


with injury_data as (
    select
        player,
        team,
        date,
        scrape_date,
        -- grabbing injury + status with the parantheses included still - ex `Out (Knee)`
        {{ split_part('description', " ' - ' ", 1) }} as injury_combined,
        {{ split_part('description', " ' - ' ", 2) }} as injury_description,
        {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 1) }} as injury_status, -- grabbing everything left of the 1st parantheses
        replace(
            replace(
                {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 2) }},
                '(', ''), ')', '') -- removing any trailing or leading parantheses
            as injury,
        case
            when {{ split_part('description', " ' - ' ", 1) }} like '%health and safety protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%Health and Safety Protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%health protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%Health Protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%protocols%' then 1
            else 0
        end as protocols,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_injury_data_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
)

select *
from injury_data