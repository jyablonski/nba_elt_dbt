SELECT player::text as player,
       team::text as team,
       season_salary::numeric as salary
FROM {{ source('nba_source', 'aws_contracts_source')}}