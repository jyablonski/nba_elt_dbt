/* add in team ranks wins, avg rank of both teams for that game etc */
with aws_schedule_table as (

    SELECT  proper_date as date,
            day_name as day,
            CONCAT(start_time, ' PM') as start_time,
    CASE WHEN home_moneyline IS NULL THEN home_team
        ELSE CONCAT(home_team, ' (', home_moneyline, ')') END as home_team,
    CASE WHEN away_moneyline IS NULL THEN away_team
        ELSE CONCAT(away_team, ' (', away_moneyline, ')') END as away_team,
            avg_team_rank
    FROM {{ ref('prep_schedule_table') }}

)

SELECT *
FROM aws_schedule_table