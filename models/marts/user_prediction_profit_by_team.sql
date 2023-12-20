{{ config(materialized='view') }}

with aggs as (
    select
        username,
        selected_winner,
        count(*) as num_bets,
        round(avg(bet_amount), 2) as avg_bet_amount,
        round(avg(bet_profit), 2) as avg_bet_profit,
        sum(case when is_correct_prediction = 1 then 1 else 0 end) as num_correct_predictions
    from {{ ref('user_past_predictions') }}
    group by
        username,
        selected_winner
)

select
    *,
    num_bets - num_correct_predictions as num_incorrect_predictions,
    round(num_correct_predictions::numeric / num_bets::numeric, 3) as correct_prediction_pct
from aggs
