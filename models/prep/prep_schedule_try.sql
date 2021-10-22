with my_cte as (
    select
        proper_date as date,
        away_team_acronym as away_team,
        home_team_acronym as home_team
from {{ ref('prep_schedule_table')}}
),

away as (
    select away_team as team,
    home_team as opp,
    date,
    'join' as join_col
    from my_cte
),

home as (
        select home_team as team,
    away_team as opp,
    date,
    'join' as join_col
    from my_cte
),

final as (
    select *
    from away
    UNION 
    select *
    from home
),

team_status as (
    select 
        team,
        team_status
    from {{ ref('prep_standings_table')}}

),

opp_status as (
    select
        team as opp,
        team_status as team_status_opp
    from {{ ref('prep_standings_table')}}
),

final2 as (
    select 
        f.team,
        f.opp,
        f.date,
        t.team_status,
        o.team_status_opp
    from final f
    left join team_status t using (team)
    left join opp_status o using (opp)
),

win_loss as (
    select 
        distinct team,
        date,
        location, 
        outcome
    from {{ ref('staging_aws_boxscores_table')}}
),

combo as (
    select *,
    case when date >= current_date then 'future' else 'past' end as game_status
    from final2
    left join win_loss using (team, date)
    order by date
)

select *
from combo

/* do counts for everything and then make prod table */