{{ config(materialized='view') }}

with home_wins as (
    select
        full_team as home_team,
        game_date,
        outcome
    from {{ ref('mov') }}
),

combo as (
    select
        id,
        username,
        user_predictions.game_date,
        user_predictions.home_team,
        home_team_odds,
        home_team_predicted_win_pct,
        away_team,
        away_team_odds,
        away_team_predicted_win_pct,
        selected_winner,
        bet_amount,
        created_at,
        case
            when outcome = 'W' then home_team
            when outcome = 'L' then away_team
            else 'TBD'
        end as actual_winner
    from {{ source('nba_prod', 'user_predictions') }}
        left join home_wins using (home_team, game_date)

),

final as (
    select
        *,
        case
            when selected_winner = actual_winner then 1
            else 0
        end as is_correct_prediction
    from combo
)


select
    *,
    case
        when is_correct_prediction = 1 then bet_amount
        else bet_amount * -1
    end as bet_profit
from final
