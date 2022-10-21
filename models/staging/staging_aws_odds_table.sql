with odds_cte as (
    select  
        team::text as team,
        replace(team, ' ', '') as team_acronym,
        spread::numeric as spread,
        total::numeric as total,
        coalesce(moneyline, 100)::numeric as moneyline, 
        datetime1::timestamp as time, 
        date::date as date
    from {{ source('nba_source', 'aws_odds_source')}}

)

select 
    team,
    {{ convert_team_names('team_acronym') }} as team_acronym,
    spread,
    total,
    moneyline,
    time, 
    date
from odds_cte