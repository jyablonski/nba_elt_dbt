SELECT
    player::text AS player,
    team::text AS team,
    coalesce(season_salary, 1500000)::numeric AS salary
FROM {{ source('nba_source', 'aws_contracts_source')}}
order by salary desc