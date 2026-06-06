with playoff_boxscore_games as (
    select distinct
        extract(year from game_date)::int as season,
        game_date,
        least(team, opponent) as team_a,
        greatest(team, opponent) as team_b
    from {{ ref('fact_boxscores') }}
    where season_type = 'Playoffs'
),

series_games as (
    select
        season,
        game_date,
        team_a,
        team_b
    from {{ ref('int_playoff_series_games') }}
)

select playoff_boxscore_games.*
from playoff_boxscore_games
    left join series_games
        on playoff_boxscore_games.season = series_games.season
        and playoff_boxscore_games.game_date = series_games.game_date
        and playoff_boxscore_games.team_a = series_games.team_a
        and playoff_boxscore_games.team_b = series_games.team_b
where series_games.game_date is null
