with odds as (
    select
        team,
        spread,
        moneyline,
        date
    from {{ ref('odds_data') }}
),

team_outcomes as (
    select
        team,
        opponent,
        game_date,
        outcome,
        mov
    from {{ ref('mov') }}
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
        inner join team_outcomes
            on
                odds.date = team_outcomes.game_date
                and odds.team = team_outcomes.team
)

select *
from final
