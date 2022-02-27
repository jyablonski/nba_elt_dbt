with schedule_data as (
    select
        away_team::text as away_team,
        /* extract(isodow from proper_date) as day_of_week, this gives the day of week in numeric form lmfao */
        home_team::text as home_team,
        date::text as date,
        proper_date::date as proper_date,
        SUBSTR(start_time, 0, LENGTH(start_time) - 0)::text as start_time,
        TO_CHAR(proper_date, 'Day') as day_name,
        'join' as join_col
    from {{ source('nba_source', 'aws_schedule_source')}}
),

away_teams as (
  select 
    s.away_team as away_team,
    a.team_acronym as away_team_acronym,
    'join' as join_col
  from {{ source('nba_source', 'aws_schedule_source')}} as s
  left join {{ ref('staging_seed_team_attributes')}} as a on s.away_team = a.team
),

home_teams as (
  select 
    s.home_team as home_team,
    a.team_acronym as home_team_acronym,
    'join' as join_col
  from {{ source('nba_source', 'aws_schedule_source')}} as s
  left join {{ ref('staging_seed_team_attributes')}} as a on s.home_team = a.team
)


select *
from schedule_data
/*
wip fixing LA - LAL and LA - LAC
left join away_teams using (join_col)
left join home_teams using (join_col)
*/