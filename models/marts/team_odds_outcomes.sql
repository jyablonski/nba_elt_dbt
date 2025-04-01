with odds_outcomes as (
    select
        team,
        season_type,
        games_played,
        games_covered_spread,
        games_favorite,
        games_underdog,
        games_favorite_covered,
        games_underdog_covered,
        pct_covered_spread,
        pct_favorite_covered,
        pct_underdog_covered
    from {{ ref('prep_team_odds_outcomes_agg') }}
)

select *
from odds_outcomes
