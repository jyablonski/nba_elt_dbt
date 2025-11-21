with team_counts as (
    select
        team,
        sum(num_games_over_twenty_pts) as total_player_boxscores_over_twenty_pts,
        count(distinct player) as unique_players_with_twenty_pt_games
    from {{ ref('int_team_players_twenty_pt_games') }}
    group by team
),

-- joining on teams dimension so we have these stats early in the season
-- if a team hasn't had a 20 pt scorer yet
teams_joined as (
    select
        dim_teams.team_acronym as team,
        team_counts.total_player_boxscores_over_twenty_pts,
        team_counts.unique_players_with_twenty_pt_games,
        int_standings_table.win_percentage,
        int_standings_table.games_played,
        round(team_counts.total_player_boxscores_over_twenty_pts / int_standings_table.games_played, 2) as avg_twenty_pt_games
    from {{ ref('dim_teams') }}
        left join team_counts
            on dim_teams.team_acronym = team_counts.team
        left join {{ ref('int_standings_table') }}
            on dim_teams.team_acronym = int_standings_table.team
    order by total_player_boxscores_over_twenty_pts desc
)

select *
from teams_joined
