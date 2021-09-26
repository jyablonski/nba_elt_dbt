{{ config(materialized='table') }}

with injury_data as (

    SELECT *
    FROM {{ ref('src_aws_injury_data_table') }}

),

team_attributes as (

    SELECT *
    FROM {{ ref('src_seed_team_attributes')}}
),

prod_adv_stats_table as (

    SELECT i.Player, i.Team, i.Date, i.Description, a.team_acronym, a.primary_color
    FROM injury_data i
    LEFT JOIN team_attributes a ON a.team = i.Team
)

select *
from prod_adv_stats_table