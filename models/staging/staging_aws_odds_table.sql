SELECT
    team::text AS team,
        {{convert_team_names('team')}} as team_acronym,
    total::numeric AS total,
    moneyline::numeric AS moneyline,
    datetime1::TIMESTAMP AS time,
    date::date AS date,
    CASE WHEN team = 'GS' THEN 'GSW'
        WHEN team = 'LA' THEN 'LAL'
        WHEN team = 'PHO' THEN 'PHX'
        WHEN team = 'CHO' THEN 'CHA'
        WHEN team = 'BRK' THEN 'BKN'
        WHEN team = 'NY' THEN 'NYK'
        ELSE team
 END
    AS team_acronym
FROM {{ source('nba_source', 'aws_odds_source')}}