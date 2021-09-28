with team_ratings as (

    SELECT *
    FROM {{ ref('stg_aws_adv_stats_table') }}

),

team_attributes as (

    SELECT *
    FROM {{ ref('stg_seed_team_attributes')}}
),

final_team_ratings as (

    SELECT 
        team_ratings.team,
        team_attributes.team_acronym,
        team_ratings.w,
        team_ratings.l,
        team_ratings.ortg,
        team_ratings.drtg,
        team_ratings.nrtg,
        CONCAT('logos/', LOWER(team_acronym), '.png')
    FROM team_ratings
    LEFT JOIN team_attributes using (team)
)

select *
from final_team_ratings