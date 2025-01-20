with most_recent_date as (
    select max(scrape_date) as max_scrape_date
    from {{ source('nba_source', 'aws_injury_data_source') }}
),

injury_data as (
    select
        player,
        team,
        date,
        aws_injury_data_source.scrape_date,
        -- grabbing injury + status with the parantheses included still - ex `Out (Knee)`
        {{ split_part('description', " ' - ' ", 1) }} as injury_combined,
        {{ split_part('description', " ' - ' ", 2) }} as injury_description,
        {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 1) }} as injury_status, -- grabbing everything left of the 1st parantheses
        replace(
            replace(
                {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 2) }},
                '(', ''
            ), ')', ''
        ) -- removing any trailing or leading parantheses
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
        inner join most_recent_date on aws_injury_data_source.scrape_date = most_recent_date.max_scrape_date
)

select *
from injury_data
