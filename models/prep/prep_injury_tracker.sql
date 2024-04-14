-- have to de-dupe them - these 2 can exist at the same time
-- Out (Knee) - The Pelicans announced that Ingram will be re-evaluated in two weeks after sustaining a bone contusion in his left knee, per Senior NBA Insider Chris Haynes.
-- Day To Day (Knee) - Ingram is questionable for Sunday's (Apr. 14) game against Houston.

with injury_data as (
    select
        player,
        injury_status,
        injury,
        row_number() over (partition by player order by modified_at desc) as row_num
    from {{ ref('injury_data') }}

),


player_stats as (
    select
        player,
        team,
        games_played,
        avg_ppg,
        avg_mvp_score,
        avg_ts_percent,
        avg_plus_minus
    from {{ ref('prep_player_stats') }}
),

player_last_game_played as (
    select
        player,
        max(game_date) as player_latest_game
    from {{ ref('boxscores') }}
    group by player

),

team_last_game_played as (
    select
        team,
        max(game_date) as team_latest_game
    from {{ ref('boxscores') }}
    group by team
),

-- used to filter out extra records and just have 1 row for each team game played
team_gp as (
    select distinct
        team,
        game_date
    from {{ ref('boxscores') }}
),

-- CTE using a windows function to make a continuous gp column for every game played by team
team_gp_continuous as (
    select
        team,
        game_date,
        rank() over (partition by team order by game_date) as continuous_games_played
    from team_gp
    group by team, game_date
),

team_gp_counts as (
    select
        team,
        count(*) as team_games_played
    from team_gp
    group by team
),

player_logo as (
    select
        player,
        headshot as player_logo
    from {{ ref('players') }}
),

final as (
    select
        player_latest_game,
        team_latest_game,
        team_gp_continuous.continuous_games_played,
        team_gp_counts.team_games_played,
        games_played,
        avg_ppg,
        avg_mvp_score,
        avg_ts_percent,
        avg_plus_minus,
        player_logo,
        concat(
            '<span style=''font-size:16px; color:royalblue;''>', injury_data.player, '</span> <span style=''font-size:12px; color:grey;''>', player_stats.team, '</span>'
        ) as player,
        concat(injury_status, ' - ', injury) as injury_status,
        team_latest_game - player_latest_game as days_missed,
        team_games_played - continuous_games_played as continuous_games_missed
    from injury_data
        inner join player_stats using (player)
        left join player_last_game_played using (player)
        left join team_last_game_played using (team)
        left join team_gp_continuous on (player_stats.team = team_gp_continuous.team and player_last_game_played.player_latest_game = team_gp_continuous.game_date)
        left join team_gp_counts on (player_stats.team = team_gp_counts.team)
        left join player_logo using (player)
    where injury_data.row_num = 1
)

select *
from final
