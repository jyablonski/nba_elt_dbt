SELECT team,
        {{convert_team_names('team')}} as team_acronym,
        spread,
        total,
        moneyline, 
        time, 
        date
FROM {{ source('nba_source', 'aws_odds_table')}}