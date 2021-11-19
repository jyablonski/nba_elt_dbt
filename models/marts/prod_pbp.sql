with pbp_table as (
    select *
    from {{ ref('prep_pbp_table')}}
),

home_vars as (
    select
        team as home_team_full,
        team_acronym as home_team,
        primary_color as home_primary_color
    from {{ ref('staging_seed_team_attributes')}}

),

away_vars as (
    select
        team as away_team_full,
        team_acronym as away_team,
        primary_color as away_primary_color
    from {{ ref('staging_seed_team_attributes')}}

),

recent_date as (
    select max(date) as date
    from {{ ref('prep_pbp_table')}}
),

final as (
    select distinct
        pbp_table.time_quarter,
        pbp_table.play,
        pbp_table.time_remaining_final,
        pbp_table.quarter,
        pbp_table.away_score,
        pbp_table.score,
        pbp_table.home_score,
        pbp_table.home_team,
        pbp_table.away_team,
        pbp_table.score_away,
        pbp_table.score_home,
        pbp_table.margin_score,
        pbp_table.date,
        pbp_table.leading_team,
        home_vars.home_team_full,
        home_vars.home_primary_color,
        away_vars.away_team_full,
        away_vars.away_primary_color,
        CONCAT(home_team_full, ' Vs. ', away_team_full) as game_description,
        CONCAT('<span style=''color:', away_primary_color, ''';>', away_team_full, '</span>') as away_fill,
        CONCAT('<span style=''color:', home_primary_color, ''';>', home_team_full, '</span>') as home_fill,
        case when pbp_table.away_score IS NULL then home_primary_color
         when pbp_table.home_score IS NULL then away_primary_color
         else '#808080' end as scoring_team_color,
        case when pbp_table.away_score IS NULL then pbp_table.home_team
         when pbp_table.home_score IS NULL then pbp_table.away_team
         else 'TIE' end as scoring_team
    from pbp_table
    left join home_vars on home_vars.home_team = pbp_table.home_team
    left join away_vars on away_vars.away_team = pbp_table.away_team
    inner join recent_date using (date)
    order by game_description, time_remaining_final desc
),

winning_team as (
    select game_description, date, MIN(time_remaining_final) as time_remaining_final,
    max(margin_score) as max_home_lead,
    min(margin_score) as max_away_lead
    from final
    group by 1, 2
),

winning_team2 as (
    select leading_team as winning_team,
    case when home_team = leading_team then away_team
    else home_team end as losing_team,
    max_home_lead,
    max_away_lead,
     game_description,
     date
    from final
    inner join winning_team using (game_description, date, time_remaining_final)
)


select 
    *
from final
left join winning_team2 using (game_description, date)
