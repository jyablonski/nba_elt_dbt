with my_cte as (
    select
        player::text as player,
        team::text as team,
        coalesce(season_salary, 1000000)::numeric as salary
    from {{ source('nba_source', 'aws_contracts_source') }}
    order by salary desc
),

/* sql baby
 have to wrap a million of these mfers to do str replacements.
 i fixed this in the python script so it's not needed anymore */
players_fixed as (
    select
        salary,
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
            player, 'Kelly Oubre Jr.', 'Kelly Oubre'
        ), 'Kira Lewis Jr.', 'Kira Lewis'),
        'Michael Porter Jr.', 'Michael Porter'), 'Mo Bamba', 'Mohamed Bamba'), 'Jaren Jackson Jr.', 'Jaren Jackson'),
        'Wendell Carter Jr.', 'Wendell Carter'), 'Kenyon Martin Jr.', 'Kenyon Martin'), 'Gary Trent Jr.', 'Gary Trent'),
        'Trey Murphy III', 'Trey Murphy'), 'Larry Nance Jr.', 'Larry Nance'), 'Gary Payton II', 'Gary Payton'),
        'Troy Brown Jr.', 'Troy Brown'), 'Kevin Porter Jr.', 'Kevin Porter'), 'Enes Kanter', 'Enes Freedom') as player
    from my_cte
)

select *
from players_fixed
