with team_attributes as (
    SELECT * 
    FROM {{ ref('seed_team_attributes')}}

)

SELECT * 
FROM team_attributes