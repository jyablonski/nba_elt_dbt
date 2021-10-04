{{ config(materialized='table') }}

with aws_adv_stats_table as (

    SELECT *
    FROM {{ ref('staging_aws_adv_stats_table') }}

),

team_attributes as (

    SELECT *
    FROM {{ ref('staging_seed_team_attributes')}}
),

prod_adv_stats_table as (

    SELECT *
    FROM aws_adv_stats_table
    LEFT JOIN team_attributes using (team)
)

select *
from prod_adv_stats_table
