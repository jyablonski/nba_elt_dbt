{{
    config(
        materialized='table',
        post_hook=[
            "insert into silver.ml_game_features_v2_audit
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

-- 1. Helper CTE: Get list of players currently OUT
-- (Used for Star Power calculation below)
current_injured_players as (
    select distinct player
    from {{ ref('fact_injury_data') }}
    where
        is_player_out = 1
),

-- 2. Anti-Join: Sum star score ONLY if player is NOT in the injured list
team_star_power_aggs as (
    select
        stats.team,
        sum(stats.star_tier_score) as team_star_score
    from {{ ref('int_player_stats') }} as stats
        left join current_injured_players as injured
            on stats.player = injured.player
    where
        -- only keep rows where the join failed (Player is NOT injured)
        injured.player is null
    group by stats.team
),

team_vorp_aggs as (
    select *
    from {{ ref('int_team_vorp_aggs') }}
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

home_team_win_pct as (
    select
        team_full as home_team,
        win_percentage as home_team_win_pct,
        case
            when (wins_last_10::numeric + losses_last_10::numeric) = 0 then 0.500
            else round((wins_last_10::numeric / (wins_last_10::numeric + losses_last_10::numeric))::numeric, 3)
        end as home_team_win_pct_last10
    from {{ ref('int_standings_table') }}
),

away_team_win_pct as (
    select
        team_full as away_team,
        win_percentage as away_team_win_pct,
        case
            when (wins_last_10::numeric + losses_last_10::numeric) = 0 then 0.500
            else round((wins_last_10::numeric / (wins_last_10::numeric + losses_last_10::numeric))::numeric, 3)
        end as away_team_win_pct_last10
    from {{ ref('int_standings_table') }}
),

final as (
    select
        games.home_team,
        games.away_team,
        games.home_moneyline,
        games.away_moneyline,
        games.game_date,

        coalesce(games.home_team_rank, 15) as home_team_rank,
        coalesce(home_rest.days_rest, 4) as home_days_rest,

        coalesce(home_team_avg_pts_scored, 112) as home_team_avg_pts_scored,
        coalesce(home_team_avg_pts_scored_opp, 112) as home_team_avg_pts_scored_opp,
        coalesce(home_team_win_pct, 0.50) as home_team_win_pct,
        coalesce(home_team_win_pct_last10, 0.50) as home_team_win_pct_last10,

        coalesce(home_stars.team_star_score, 0) as home_star_score,
        coalesce(home_vorp.team_active_vorp, 0) as home_active_vorp,
        coalesce(home_vorp.pct_vorp_missing, 0) as home_pct_vorp_missing,

        coalesce(home_travel.travel_miles_last_7_days, 0) as home_travel_miles_last_7_days,
        coalesce(home_travel.games_last_7_days, 0) as home_games_last_7_days,
        case when home_travel.is_cross_country_trip then 1 else 0 end as home_is_cross_country_trip,

        coalesce(games.away_team_rank, 15) as away_team_rank,
        coalesce(away_rest.days_rest, 4) as away_days_rest,
        coalesce(away_team_avg_pts_scored, 112) as away_team_avg_pts_scored,
        coalesce(away_team_avg_pts_scored_opp, 112) as away_team_avg_pts_scored_opp,
        coalesce(away_team_win_pct, 0.50) as away_team_win_pct,
        coalesce(away_team_win_pct_last10, 0.50) as away_team_win_pct_last10,

        coalesce(away_stars.team_star_score, 0) as away_star_score,
        coalesce(away_vorp.team_active_vorp, 0) as away_active_vorp,
        coalesce(away_vorp.pct_vorp_missing, 0) as away_pct_vorp_missing,

        coalesce(away_travel.travel_miles_last_7_days, 0) as away_travel_miles_last_7_days,
        coalesce(away_travel.games_last_7_days, 0) as away_games_last_7_days,
        case when away_travel.is_cross_country_trip then 1 else 0 end as away_is_cross_country_trip,

        coalesce(home_travel.travel_miles_last_7_days, 0) - coalesce(away_travel.travel_miles_last_7_days, 0) as travel_miles_differential,
        coalesce(home_stars.team_star_score, 0) - coalesce(away_stars.team_star_score, 0) as star_score_differential,
        coalesce(home_vorp.team_active_vorp, 0) - coalesce(away_vorp.team_active_vorp, 0) as active_vorp_differential,

        null as outcome

    from games
        left join home_team_avg using (home_team)
        left join home_team_win_pct using (home_team)
        left join away_team_avg using (away_team)
        left join away_team_win_pct using (away_team)

        left join {{ ref('int_team_games_days_rest') }} as home_rest
            on games.home_team = home_rest.team
            and games.game_date = home_rest.game_date
        left join {{ ref('int_team_games_days_rest') }} as away_rest
            on games.away_team = away_rest.team
            and games.game_date = away_rest.game_date

        left join {{ ref('int_team_travel_schedule') }} as home_travel
            on games.home_team_acronym = home_travel.team
            and games.game_date = home_travel.game_date
        left join {{ ref('int_team_travel_schedule') }} as away_travel
            on games.away_team_acronym = away_travel.team
            and games.game_date = away_travel.game_date

        left join team_star_power_aggs as home_stars
            on games.home_team_acronym = home_stars.team
        left join team_star_power_aggs as away_stars
            on games.away_team_acronym = away_stars.team

        left join team_vorp_aggs as home_vorp
            on games.home_team_acronym = home_vorp.team
        left join team_vorp_aggs as away_vorp
            on games.away_team_acronym = away_vorp.team
)

select *
from final
