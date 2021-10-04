{{ config(materialized='table') }}

with injury_data as (

    SELECT *
    FROM {{ ref('staging_aws_injury_data_table') }}

),

team_attributes as (

    SELECT *
    FROM {{ ref('staging_seed_team_attributes')}}
),

prod_adv_stats_table as (

    SELECT 
        injury_data.player,
        injury_data.team, 
        injury_data.description, 
        injury_data.date,
        team_attributes.team_acronym,
        team_attributes.primary_color
    FROM injury_data
    LEFT JOIN team_attributes using (team)
)

select *
from prod_adv_stats_table