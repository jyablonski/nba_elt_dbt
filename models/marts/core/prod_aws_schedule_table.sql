{{ config(materialized='table') }}

with aws_schedule_table as (

    SELECT SUBSTR(start_time, 0, LENGTH(start_time) - 0) as start_time,
    /* extract(isodow from proper_date) as day_of_week, this gives the day of week in numeric form lmfao */
    to_char(proper_date, 'Day') as day_name,
    away_team,
    home_team,
    date,
    proper_date
    FROM {{ ref('stg_aws_schedule_table') }}

)

SELECT *
FROM aws_schedule_table