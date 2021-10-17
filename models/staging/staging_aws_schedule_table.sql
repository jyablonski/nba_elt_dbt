with schedule_data as (
    SELECT SUBSTR(start_time, 0, LENGTH(start_time) - 0)::text as start_time,
            /* extract(isodow from proper_date) as day_of_week, this gives the day of week in numeric form lmfao */
            to_char(proper_date, 'Day') as day_name,
            away_team::text as away_team,
            home_team::text as home_team,
            date::text as date,
            proper_date::date as proper_date
    FROM {{ source('nba_source', 'aws_schedule_source')}}
)

SELECT *
FROM schedule_data