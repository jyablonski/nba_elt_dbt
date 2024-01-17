/* Day To Day (Shoulder) - Gallinari is questionable for Thursday's (Oct. 21) game against Dallas. */

with injury_data as (
    select
        player,
        team,
        date,
        scrape_date,
        {{ dbt_utils.split_part('description', " ' - ' ", 1) }} as injury, /* grabbing injury + status with the parantheses included still */
        {{ dbt_utils.split_part('description', " ' - ' ", 2) }} as description, /* grabbing description */
        case
            when {{ dbt_utils.split_part('description', " ' - ' ", 1) }} like '%health and safety protocols%' then 1
            when {{ dbt_utils.split_part('description', " ' - ' ", 1) }} like '%Health and Safety Protocols%' then 1
            when {{ dbt_utils.split_part('description', " ' - ' ", 1) }} like '%health protocols%' then 1
            when {{ dbt_utils.split_part('description', " ' - ' ", 1) }} like '%Health Protocols%' then 1
            when {{ dbt_utils.split_part('description', " ' - ' ", 1) }} like '%protocols%' then 1
            else 0
        end as protocols
    from {{ source('nba_source', 'aws_injury_data_source') }}
),

injury_data2 as (
    select
        *,
        {{ dbt_utils.split_part('injury', " ' ('", 1) }} as status, /* grabbing everything left of the 1st parantheses */
        {{ dbt_utils.split_part('injury', " ' ('", 2) }} as injury2 /* grabbing the actual injury */
    from injury_data
),

team_attributes as (

    select
        team,
        team_acronym
    from {{ ref('staging_seed_team_attributes') }}
),

most_recent_date as (
    select max(scrape_date) as scrape_date
    from {{ source('nba_source', 'aws_injury_data_source') }}
),

injury_counts as (
    select
        team,
        count(*) as team_active_injuries
    from injury_data
        inner join most_recent_date using (scrape_date)
    group by 1
),

protocol_counts as (
    select
        team,
        count(*) as team_active_protocols
    from injury_data
        inner join most_recent_date using (scrape_date)
    where protocols = 1
    group by 1
),

final_stg_injury as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['player', 'injury', 'description']) }} as injury_pk,
        injury_data2.player,
        team_attributes.team_acronym,
        injury_data2.team,
        injury_data2.date,
        injury_data2.status,
        replace(replace(injury_data2.injury2, '(', ''), ')', '') as injury, /* removing left AND right parantheses */
        injury_data2.description,
        injury_counts.team_active_injuries as total_injuries,
        injury_counts.team_active_injuries - cast(coalesce(protocol_counts.team_active_protocols, 0) as numeric) as team_active_injuries,
        cast(coalesce(protocol_counts.team_active_protocols, 0) as numeric) as team_active_protocols,
        injury_data2.scrape_date,
        row_number() over (partition by player order by date desc) as uniqueness_filter
    from injury_data2
        left join team_attributes using (team)
        left join injury_counts using (team)
        left join protocol_counts using (team)
        inner join most_recent_date using (scrape_date)

),

-- 2023-03-09 adding this in bc bbref duplicates their fkn injury records and it causes pk issues
final as (
    select
        injury_pk,
        player,
        team_acronym,
        team,
        date,
        status,
        injury,
        description,
        total_injuries,
        team_active_injuries,
        team_active_protocols,
        scrape_date
    from final_stg_injury
    where uniqueness_filter = 1
)

select *
from final
