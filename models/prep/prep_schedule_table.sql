/* got stuck - l;eft hjere 
https://stackoverflow.com/questions/19601948/must-appear-in-the-group-by-clause-or-be-used-in-an-aggregate-function
need to filter odds data to the correct date given */


with schedule_data as (
    SELECT *,
            CASE WHEN start_time = '' THEN '8:00' /* this was for empty values - im setting a default here bc fk it */
            ELSE start_time
            END AS start_time2
    FROM {{ ref('staging_aws_schedule_table')}}
),

home_team_attributes as (
    SELECT  a.team as home_team,
            a.team_acronym as home_team_acronym,
            a.previous_season_rank as home_team_prev_rank
    FROM {{ ref('staging_seed_team_attributes')}} a
),

home_team_odds as (
    SELECT  team_acronym as home_team_acronym,
            (array_agg(moneyline ORDER BY date DESC))[1] as home_moneyline, /* grabs the most recent moneyline odds */
            max(date) as proper_date
    FROM {{ ref('staging_aws_odds_table')}}
    GROUP BY home_team_acronym
),

away_team_odds as (
    SELECT  team_acronym as away_team_acronym,
            (array_agg(moneyline ORDER BY date DESC))[1] as away_moneyline, /* grabs the most recent moneyline odds */
            max(date) as proper_date
    FROM {{ ref('staging_aws_odds_table')}}
    GROUP BY away_team_acronym
),

away_team_attributes as (
    SELECT  team as away_team,
            team_acronym as away_team_acronym,
            previous_season_rank as away_team_prev_rank
    FROM {{ ref('staging_seed_team_attributes')}}
),

final_table as (
    SELECT  s.start_time2 as start_time,
        s.day_name,
        s.away_team,
        s.home_team,
        s.date,
        s.proper_date,
        h.home_team_acronym,
        h.home_team_prev_rank,
        a.away_team_acronym,
        a.away_team_prev_rank,
        (a.away_team_prev_rank + h.home_team_prev_rank) / 2 as avg_team_rank,
        ho.home_moneyline,
        ao.away_moneyline
FROM schedule_data s
LEFT JOIN home_team_attributes h using (home_team)
LEFT JOIN away_team_attributes a using (away_team)
LEFT JOIN home_team_odds ho on h.home_team_acronym = ho.home_team_acronym and s.proper_date = ho.proper_date
LEFT JOIN away_team_odds ao on a.away_team_acronym = ao.away_team_acronym and s.proper_date = ao.proper_date
order by proper_date asc
)

SELECT *,
        CONCAT(proper_date::text, ' ', start_time::text, ':00')::timestamp as proper_time
FROM final_table
ORDER BY proper_time

/* WIP 
,
        CASE WHEN ho.home_moneyline > 0 THEN CONCAT('+', ho.home_moneyline::text)
                ELSE ho.home_moneyline END as home_moneyline2,
        CASE WHEN ao.away_moneyline > 0 THEN CONCAT('+', ao.away_moneyline::text)
                ELSE ao.away_moneyline END as away_moneyline2
                */ 