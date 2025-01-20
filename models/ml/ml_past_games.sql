{{ config(materialized='view') }}

with games as (
    select
        home_team,
        away_team,
        home_team_acronym,
        away_team_acronym,
        game_date,
        home_team_rank,
        away_team_rank,
        home_days_rest,
        away_days_rest
    from {{ ref('prep_schedule_table') }}
    where game_date < date({{ dbt.current_timestamp() }} - interval '6 hour')
),

outcomes as (
    select distinct
        dim_teams.team as home_team,
        fact_boxscores.game_date,
        case when fact_boxscores.outcome = 'W' then 1 else 0 end as outcome
    from {{ ref('fact_boxscores') }}
        left join {{ ref('dim_teams') }} on fact_boxscores.team = dim_teams.team_acronym
    where fact_boxscores.location = 'H'
),

home_team_avg as (
    select
        full_team as home_team,
        round(avg(pts_scored), 1)::numeric as home_team_avg_pts_scored,
        round(avg(pts_scored_opp), 1)::numeric as home_team_avg_pts_scored_opp
    from {{ ref('prep_recent_games_teams') }}
    group by full_team

),

away_team_avg as (
    select
        full_team as away_team,
        round(avg(pts_scored), 1)::numeric as away_team_avg_pts_scored,
        round(avg(pts_scored_opp), 1)::numeric as away_team_avg_pts_scored_opp
    from {{ ref('prep_recent_games_teams') }}
    group by full_team

),

-- home team win pct
-- away team win pct

-- last 20 games home/away win pcts

home_team_win_pct as (
    select
        team_full as home_team,
        win_percentage as home_team_win_pct,
        round(wins_last_10::numeric / (wins_last_10::numeric + losses_last_10::numeric), 3)::numeric as home_team_win_pct_last10
    from {{ ref('prep_standings_table') }}
),

-- this last10 isnt accurate bc it doesnt take the value at that point of time, it's just the value as of today
away_team_win_pct as (
    select
        team_full as away_team,
        win_percentage as away_team_win_pct,
        round(wins_last_10::numeric / (wins_last_10::numeric + losses_last_10::numeric), 3)::numeric as away_team_win_pct_last10
    from {{ ref('prep_standings_table') }}
),

home_team_top_players as (
    select
        team as home_team_acronym,
        game_date,
        is_top_players as home_is_top_players
    from {{ ref('prep_top_players_present') }}
),

away_team_top_players as (
    select
        team as away_team_acronym,
        game_date,
        is_top_players as away_is_top_players
    from {{ ref('prep_top_players_present') }}
),

final as (
    select
        home_team,
        away_team,
        game_date,
        home_team_rank,
        home_days_rest,
        home_team_avg_pts_scored,
        home_team_avg_pts_scored_opp,
        home_team_win_pct,
        home_team_win_pct_last10,
        home_is_top_players,
        away_team_rank,
        away_days_rest,
        away_team_avg_pts_scored,
        away_team_avg_pts_scored_opp,
        away_team_win_pct,
        away_team_win_pct_last10,
        away_is_top_players,
        outcome
    from games
        left join home_team_avg using (home_team)
        left join home_team_win_pct using (home_team)
        left join home_team_top_players using (home_team_acronym, game_date)
        left join away_team_avg using (away_team)
        left join away_team_win_pct using (away_team)
        left join away_team_top_players using (away_team_acronym, game_date)
        left join outcomes using (home_team, game_date)
    where outcome is not null
)

-- outcome == 1 means home team won,
-- outcome == 0 means away team won
select *
from final
