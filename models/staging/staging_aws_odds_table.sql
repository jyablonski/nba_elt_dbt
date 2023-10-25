with odds_cte as (
    select
        team::text as team,
        spread::numeric as spread,
        total::numeric as total,
        coalesce(moneyline, 100)::numeric as moneyline,
        datetime1::timestamp as time,
        date::date as date,
        replace(team, ' ', '') as team_acronym
    from {{ source('nba_source', 'aws_odds_source') }}
    where date >= '2023-10-01'

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
