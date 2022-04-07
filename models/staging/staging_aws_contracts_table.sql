with my_cte as (
    select
        player::text as player,
        team::text as team,
        coalesce(season_salary, 1000000)::numeric as salary
    from {{ source('nba_source', 'aws_contracts_source')}}
    order by salary desc
),

/* sql baby 
 have to wrap a million of these mfers to do str replacements.
 i fixed this in the python script so it's not needed anymore */
players_fixed as (
    select 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            player, 'Kelly Oubre Jr.', 'Kelly Oubre'), 'Kira Lewis Jr.', 'Kira Lewis'),
            'Michael Porter Jr.', 'Michael Porter'), 'Mo Bamba', 'Mohamed Bamba'), 'Jaren Jackson Jr.', 'Jaren Jackson'),
            'Wendell Carter Jr.', 'Wendell Carter'), 'Kenyon Martin Jr.', 'Kenyon Martin'), 'Gary Trent Jr.', 'Gary Trent'),
            'Trey Murphy III', 'Trey Murphy'), 'Larry Nance Jr.', 'Larry Nance'), 'Gary Payton II', 'Gary Payton'),
            'Troy Brown Jr.', 'Troy Brown'), 'Kevin Porter Jr.', 'Kevin Porter'), 'Enes Kanter', 'Enes Freedom') as player,
    team,
    salary
    from my_cte
),

-- this is to grab the team the player has MOST RECENTLY played for
players_date as (
    select 
        player,
        max(date) as date
    from {{ ref('staging_aws_boxscores_table')}}
    group by 1
),

-- joining that most recent team
players_team as (
    select
        b.player,
        date,
        b.team as new_team
    from {{ ref('staging_aws_boxscores_table')}} as b
    inner join players_date using (date)
),

combo as (
    select 
        distinct f.player,
        f.team,
        n.new_team,
        f.salary
    from players_fixed as f
    left join players_team as n using (player)
    order by player
),

final as (
    select 
        distinct player,
        coalesce(new_team, team) as team, /* use team from boxscores with most recent date, if null then use the team from contracts web scrape */
        salary
    from combo
    order by player
)

select 
    *
from final