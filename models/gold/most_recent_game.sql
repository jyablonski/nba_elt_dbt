select max(game_date) as max_game_date
from {{ ref('fact_boxscores') }}
