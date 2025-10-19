-- this test checks the same view that is used for the betting page
-- with the goal of ensuring that all past games are accounted for.
-- if any historical games are still `TBD` then there was an ingestion issue

with missing_games as (
    select *
    from {{ ref('user_past_predictions') }}
    where
        game_date >= '{{ var("prediction_start_date") }}'
        and game_date < current_date
        and actual_winner = 'TBD'
)

select *
from missing_games
