with my_cte as (
    select
        game_date,
        away_team,
        home_team,
        outcome,
        ml_accuracy,
        home_team_predicted_win_pct,
        home_implied_probability,
        away_team_predicted_win_pct,
        away_implied_probability,
        round(home_implied_probability::numeric - home_team_predicted_win_pct::numeric, 3) as home_probability_diff,
        round(away_implied_probability::numeric - away_team_predicted_win_pct::numeric, 3) as away_probability_diff,
        abs(round(home_implied_probability::numeric - home_team_predicted_win_pct::numeric, 3))
        +
        abs(round(away_implied_probability::numeric - away_team_predicted_win_pct::numeric, 3))
        as combined_probability_diff
    from {{ ref('ml_past_games_odds_analysis') }}
)

select *
from my_cte
order by combined_probability_diff desc
