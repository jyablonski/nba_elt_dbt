{{ config(materialized='table') }}

with aws_adv_stats_table as (

    SELECT *
    FROM {{ ref('src_aws_adv_stats_table') }}

),

team_attributes as (

    SELECT *
    FROM {{ ref('seed_team_attributes')}}
),

prod_adv_stats_table as (

    SELECT s.team, a.urlthumbnailteam
    FROM aws_adv_stats_table s
    LEFT JOIN team_attributes a using (team)
)

select *
from prod_adv_stats_table
