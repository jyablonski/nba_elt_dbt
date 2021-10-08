/* got stuck - l;eft hjere 
https://stackoverflow.com/questions/19601948/must-appear-in-the-group-by-clause-or-be-used-in-an-aggregate-function
need to filter odds data to the correct date given */


with schedule_data as (
    SELECT *
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
            (array_agg(moneyline ORDER BY date DESC))[1] as home_moneyline,
            max(date) as date
    FROM {{ ref('staging_aws_odds_table')}}
    GROUP BY home_team_acronym
),

away_team_odds as (
    SELECT  team_acronym as away_team_acronym,
            (array_agg(moneyline ORDER BY date DESC))[1] as away_moneyline,
            max(date) as date
    FROM {{ ref('staging_aws_odds_table')}}
    GROUP BY away_team_acronym
),

away_team_attributes as (
    SELECT  team as away_team,
            team_acronym as away_team_acronym,
            previous_season_rank as away_team_prev_rank
    FROM {{ ref('staging_seed_team_attributes')}}
)

SELECT  s.start_time,
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
LEFT JOIN home_team_odds ho using (home_team_acronym)
LEFT JOIN away_team_odds ao using (away_team_acronym)
