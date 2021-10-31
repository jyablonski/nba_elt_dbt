/* Day To Day (Shoulder) - Gallinari is questionable for Thursday's (Oct. 21) game against Dallas. */

with injury_data as (
    SELECT player, team, date, scrape_date,
        {{dbt_utils.split_part('description', " ' - ' ", 1)}} as injury, /* grabbing injury + status with the parantheses included still */
        {{dbt_utils.split_part('description', " ' - ' ", 2)}} as description /* grabbing description */
    FROM {{ source('nba_source', 'aws_injury_data_source')}}
),

injury_data2 as (
    SELECT *,
            {{dbt_utils.split_part('injury', " ' ('", 1)}} as status, /* grabbing everything left of the 1st parantheses */
            {{dbt_utils.split_part('injury', " ' ('", 2)}} as injury2 /* grabbing the actual injury */
    FROM injury_data
),

team_attributes as (

    SELECT team,
           team_acronym
    FROM {{ ref('staging_seed_team_attributes')}}
),

most_recent_date as (
    select max(scrape_date) as scrape_date
    from {{ source('nba_source', 'aws_injury_data_source')}}
),

injury_counts as (
    SELECT team,
           count(*) as team_active_injuries
    FROM injury_data
    inner join most_recent_date using (scrape_date)
    GROUP BY 1
),

final_stg_injury as (
    SELECT injury_data2.player,
           team_attributes.team_acronym,
           injury_data2.team,
           injury_data2.date,
           injury_data2.status,
           replace(replace(injury_data2.injury2, '(', ''), ')', '') as injury, /* removing left AND right parantheses */
           injury_data2.description,
           injury_counts.team_active_injuries,
           injury_data2.scrape_date
    FROM injury_data2
    LEFT JOIN team_attributes using (team)
    LEFT JOIN injury_counts using (team)
    inner join most_recent_date using (scrape_date)

)

SELECT * FROM final_stg_injury