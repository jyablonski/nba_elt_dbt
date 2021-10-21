with my_cte as (
    SELECT
        player::text AS player,
        team::text AS team,
        coalesce(season_salary, 1000000)::numeric AS salary
    FROM {{ source('nba_source', 'aws_contracts_source')}}
    order by salary desc
),

/* sql baby */
players_fixed as (
    select 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            player, 'Kelly Oubre Jr.', 'Kelly Oubre'), 'Kira Lewis Jr.', 'Kira Lewis'),
            'Michael Porter Jr.', 'Michael Porter'), 'Mo Bamba', 'Mohamed Bamba'), 'Jaren Jackson Jr.', 'Jaren Jackson'),
            'Wendell Carter Jr.', 'Wendell Carter'), 'Kenyon Martin Jr.', 'Kenyon Martin'), 'Gary Trent Jr.', 'Gary Trent'),
            'Trey Murphy III', 'Trey Murphy'), 'Larry Nance Jr.', 'Larry Nance'), 'Gary Payton II', 'Gary Payton'),
            'Troy Brown Jr.', 'Troy Brown'), 'Kevin Porter Jr.', 'Kevin Porter') as player,
    team,
    salary
    from my_cte
)

select distinct player,
    salary
from players_fixed