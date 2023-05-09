with odds as (
    select
        team,
        spread,
        moneyline,
        date
    from {{ ref('staging_aws_odds_table') }}
),

team_outcomes as (
    select
        team,
        opponent,
        date,
        outcome,
        mov
    from {{ ref('prod_mov') }}
),

final as (
    select
        odds.team,
        team_outcomes.opponent,
        odds.spread,
        odds.moneyline,
        odds.date,
        team_outcomes.outcome,
        team_outcomes.mov
    from odds
    inner join team_outcomes using (team, date)
)

select *
from final