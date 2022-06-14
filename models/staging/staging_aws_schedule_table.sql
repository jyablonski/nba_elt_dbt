-- this gets complicated around playoffs, in future add scrape_date and scrape_ts to make this easier
-- no way of knowing playoff game data unlike regular season, so it has to get scraped every day around march/april
with schedule_data as (
    select distinct -- distinct to filter out nulls
        away_team::text as away_team,
        /* extract(isodow from proper_date) as day_of_week, this gives the day of week in numeric form lmfao */
        home_team::text as home_team,
        date::text as date,
        proper_date::date as proper_date,
        SUBSTR(start_time, 0, LENGTH(start_time) - 0)::text as start_time,
        TO_CHAR(proper_date, 'Day') as day_name,
        'join' as join_col
    from {{ source('nba_source', 'aws_schedule_source')}}
    where start_time like '%:%' -- hack to only get records that have a start time (7:00)
)

select *
from schedule_data
where start_time != '11:00' -- bug, thx bbref
order by proper_date desc
/*
wip fixing LA - LAL and LA - LAC
left join away_teams using (join_col)
left join home_teams using (join_col)
*/