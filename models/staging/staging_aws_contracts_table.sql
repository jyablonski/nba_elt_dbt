SELECT
    player::text AS player,
    team::text AS team,
    season_salary::numeric AS salary
FROM {{ source('nba_source', 'aws_contracts_source')}}