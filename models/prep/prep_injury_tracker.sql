with injury_data as (
    select
        player,
        status,
        injury
    from {{ ref('staging_aws_injury_data_table') }}

),

player_stats as (
    select
        player,
        team,
        games_played,
        season_avg_ppg,
        player_mvp_calc_avg,
        season_ts_percent,
        season_avg_plusminus
    from {{ ref('prep_player_aggs') }}
),

player_last_game_played as (
    select
        player,
        max(date) as player_latest_game
    from {{ ref('prep_boxscores_mvp_calc') }}
    group by player

),

team_last_game_played as (
    select
        team,
        max(date) as team_latest_game
    from {{ ref('prep_boxscores_mvp_calc') }}
    group by team
),

-- used to filter out extra records and just have 1 row for each team game played
team_gp as (
    select distinct
        team,
        date
    from {{ ref('prep_boxscores_mvp_calc') }}
),

-- CTE using a windows function to make a continuous gp column for every game played by team
team_gp_continuous as (
    select
        team,
        date,
        rank() over (partition by team order by date) as continuous_games_played
    from team_gp
    group by team, date
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
    from {{ source('nba_source', 'player_attributes') }}
),

final as (
    select
        player_latest_game,
        team_latest_game,
        team_gp_continuous.continuous_games_played,
        team_gp_counts.team_games_played,
        games_played,
        season_avg_ppg,
        player_mvp_calc_avg,
        season_ts_percent,
        season_avg_plusminus,
        player_logo,
        concat(
            '<span style=''font-size:16px; color:royalblue;''>', injury_data.player, '</span> <span style=''font-size:12px; color:grey;''>', player_stats.team, '</span>'
        ) as player,
        concat(status, ' - ', injury) as status,
        team_latest_game - player_latest_game as days_missed,
        team_games_played - continuous_games_played as continuous_games_missed
    from injury_data
        inner join player_stats using (player)
        left join player_last_game_played using (player)
        left join team_last_game_played using (team)
        left join team_gp_continuous on (player_stats.team = team_gp_continuous.team and player_last_game_played.player_latest_game = team_gp_continuous.date)
        left join team_gp_counts on (player_stats.team = team_gp_counts.team)
        left join player_logo using (player)
)

select *
from final
