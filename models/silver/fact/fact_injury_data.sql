with most_recent_date as (
    select max(scrape_date) as max_scrape_date
    from {{ source('bronze', 'bbref_player_injuries') }}
),

injury_data as (
    select
        player,
        team,
        date,
        bbref_player_injuries.scrape_date,
        -- grabbing injury + status with the parantheses included still - ex `Out (Knee)`
        {{ split_part('description', " ' - ' ", 1) }} as injury_combined,
        {{ split_part('description', " ' - ' ", 2) }} as injury_description,
        {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 1) }} as injury_status, -- grabbing everything left of the 1st parantheses
        replace(
            replace(
                {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 2) }},
                '(', ''
            ), ')', ''
        ) as injury, -- removing any trailing or leading parantheses
        case
            when {{ split_part('description', " ' - ' ", 1) }} like '%health and safety protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%Health and Safety Protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%health protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%Health Protocols%' then 1
            when {{ split_part('description', " ' - ' ", 1) }} like '%protocols%' then 1
            else 0
        end as protocols,
        case
            when {{ split_part(split_part('description', " ' - ' ", 1), " ' ('", 1) }} ilike '%out%' then 1
            else 0
        end as is_player_out,
        created_at,
        modified_at
    from {{ source('bronze', 'bbref_player_injuries') }}
        inner join most_recent_date on bbref_player_injuries.scrape_date = most_recent_date.max_scrape_date
),

deduped_injury_data as (
    select
        injury_data.*,
        row_number() over (
            partition by injury_data.player, injury_data.team, injury_data.injury_combined
            order by
                case
                    when injury_data.date::text ~ '^[A-Z][a-z]{2}, [A-Z][a-z]{2} [0-9]{1,2}, [0-9]{4}$'
                        then to_date(injury_data.date::text, 'Dy, Mon DD, YYYY')
                end desc nulls last,
                injury_data.modified_at desc nulls last,
                injury_data.created_at desc nulls last
        ) as row_num
    from injury_data
)

select
    player,
    team,
    date,
    scrape_date,
    injury_combined,
    injury_description,
    injury_status,
    injury,
    protocols,
    is_player_out,
    created_at,
    modified_at
from deduped_injury_data
where row_num = 1
