with injury_data as (
    SELECT player, team, date,
        {{dbt_utils.split_part('description', " ' - ' ", 1)}} as injury,
        {{dbt_utils.split_part('description', " ' - ' ", 2)}} as description
    FROM {{ source('nba_source', 'aws_injury_data_table')}}
),

team_attributes as (

    SELECT team,
           team_acronym
    FROM {{ ref('staging_seed_team_attributes')}}
),

injury_counts as (
    SELECT team,
           count(*) as team_active_injuries
    FROM injury_data
    GROUP BY 1
),

final_stg_injury as (
    SELECT injury_data.player,
           team_attributes.team_acronym,
           injury_data.team,
           injury_data.date,
           injury_data.injury,
           injury_data.description,
           injury_counts.team_active_injuries
    FROM injury_data
    LEFT JOIN team_attributes using (team)
    LEFT JOIN injury_counts using (team)

)

SELECT * FROM final_stg_injury