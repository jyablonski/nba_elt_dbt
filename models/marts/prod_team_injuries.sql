with final_table as (
    SELECT  player,
            team_acronym,
            team,
            date,
            status,
            injury,
            description
    FROM {{ ref('staging_aws_injury_data_table')}}
)

SELECT *
FROM final_table