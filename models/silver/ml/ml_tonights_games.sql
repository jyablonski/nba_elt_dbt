{{
    config(
        materialized='table',
        post_hook=[
            "insert into silver.ml_tonights_games_audit
             select 
                 *,
                 current_timestamp as audit_inserted_at
             from {{ this }}"
        ]
    )
}}

with games as (
    select
        home_team,
        home_moneyline,
        away_team,
        away_moneyline,
        home_team_acronym,
        away_team_acronym,
        game_date,
        home_team_rank,
        away_team_rank
    from {{ ref('int_schedule_table') }}
    where game_date = date({{ dbt.current_timestamp() }} - interval '6 hour')
),

outcomes as (
    select distinct
        dim_teams.team as home_team,
        fact_boxscores.game_date,
        case when fact_boxscores.outcome = 'W' then 1 else 0 end as outcome
    from {{ ref('fact_boxscores') }} as fact_boxscores
        left join {{ ref('dim_teams') }} as dim_teams
            on fact_boxscores.team = dim_teams.team_acronym
    where fact_boxscores.location = 'H'
),

home_team_avg as (
    select
        full_team as home_team,
        round(avg(pts_scored), 1)::numeric as home_team_avg_pts_scored,
        round(avg(pts_scored_opp), 1)::numeric as home_team_avg_pts_scored_opp
    from {{ ref('int_recent_games_teams') }}
    group by full_team

),

away_team_avg as (
    select
        full_team as away_team,
        round(avg(pts_scored), 1)::numeric as away_team_avg_pts_scored,
        round(avg(pts_scored_opp), 1)::numeric as away_team_avg_pts_scored_opp
    from {{ ref('int_recent_games_teams') }}
    group by full_team

),

-- home team win pct
-- away team win pct

-- last 20 games home/away win pcts

home_team_win_pct as (
    select
        team_full as home_team,
        win_percentage as home_team_win_pct,
        case
            when (wins_last_10::numeric + losses_last_10::numeric) = 0 then 0.500
            else round(wins_last_10::numeric / (wins_last_10::numeric + losses_last_10::numeric), 3)::numeric
        end as home_team_win_pct_last10
    from {{ ref('int_standings_table') }}
),

-- this last10 isnt accurate bc it doesnt take the value at that point of time, it's just the value as of today
away_team_win_pct as (
    select
        team_full as away_team,
        win_percentage as away_team_win_pct,
        case
            when (wins_last_10::numeric + losses_last_10::numeric) = 0 then 0.500
            else round(wins_last_10::numeric / (wins_last_10::numeric + losses_last_10::numeric), 3)::numeric
        end as away_team_win_pct_last10
    from {{ ref('int_standings_table') }}
),

-- these 2 have to be different for tonights games - pull from injury report
team_top_players as (
    select
        fact_injury_data.player,
        fact_injury_data.team,
        dim_players.rank as player_rank
    from {{ ref('fact_injury_data') }} as fact_injury_data
        inner join {{ ref('dim_players') }} as dim_players
            using (player)
    where
        fact_injury_data.injury_status != 'Day To Day'
        and dim_players.rank != 0 -- don't care players who are not in the top 2
-- use status != daytoday bc these players will most likely play anyways, so assume they're healthy.
),

home_team_top_players_aggs as (
    select
        team as home_team,
        2 - count(*) as home_is_top_players
    from team_top_players
    group by team
),

away_team_top_players_aggs as (
    select
        team as away_team,
        2 - count(*) as away_is_top_players
    from team_top_players
    group by team
),

-- home days rest away days rest has to come from different methodology than ml_past_games

home_days_rest as (
    select
        team as home_team,
        game_date as home_last_played_date
    from {{ ref('int_team_days_rest') }}
    where rank = 1
),

away_days_rest as (
    select
        team as away_team,
        game_date as away_last_played_date
    from {{ ref('int_team_days_rest') }}
    where rank = 1
),

-- coalescing a shit ton of defaults if it's like the first game of the season
final as (
    select
        home_team,
        away_team,
        home_moneyline,
        away_moneyline,
        game_date::date as game_date,
        coalesce(home_team_rank, 15) as home_team_rank,
        coalesce((game_date - home_last_played_date) - 1, 4) as home_days_rest,
        coalesce(home_team_avg_pts_scored, 112) as home_team_avg_pts_scored,
        coalesce(home_team_avg_pts_scored_opp, 112) as home_team_avg_pts_scored_opp,
        coalesce(home_team_win_pct, 0.50) as home_team_win_pct,
        coalesce(home_team_win_pct_last10, 0.50) as home_team_win_pct_last10,
        coalesce(home_is_top_players, 2)::numeric as home_is_top_players,
        coalesce(away_team_rank, 15) as away_team_rank, -- if top players missing then they're HEALTHY
        coalesce((game_date - away_last_played_date) - 1, 4) as away_days_rest,
        coalesce(away_team_avg_pts_scored, 112) as away_team_avg_pts_scored,
        coalesce(away_team_avg_pts_scored_opp, 112) as away_team_avg_pts_scored_opp,
        coalesce(away_team_win_pct, 0.50) as away_team_win_pct,
        coalesce(away_team_win_pct_last10, 0.50) as away_team_win_pct_last10,
        coalesce(away_is_top_players, 2)::numeric as away_is_top_players,
        outcome
    from games
        left join home_team_avg using (home_team)
        left join home_team_win_pct using (home_team)
        left join home_team_top_players_aggs using (home_team)
        left join away_team_avg using (away_team)
        left join away_team_win_pct using (away_team)
        left join away_team_top_players_aggs using (away_team)
        left join outcomes using (home_team, game_date)
        left join home_days_rest using (home_team)
        left join away_days_rest using (away_team)
)

-- outcome == 1 means home team won,
-- outcome == 0 means away team won
select *
from final
