{{ config(materialized='table') }}

/* add in team ranks wins, avg rank of both teams for that game etc */
with aws_schedule_table as (

    SELECT *
    FROM {{ ref('stg_aws_schedule_table') }}

)

SELECT *
FROM aws_schedule_table