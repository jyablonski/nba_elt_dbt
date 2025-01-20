select *
from {{ ref('fact_boxscores') }}
where
    team not in (
        'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
        'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
        'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
    )

/* the idea being that we want this to return 0 rows, if something gets returned that means its passing further downstream which is a problem
   12-27-21 update: this is actually not needed bc it can just be done via a column value test w/ dbt expectations */
