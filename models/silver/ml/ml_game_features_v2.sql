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
        and dim_players.rank != 0
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

final as (
    select
        games.home_team,
        games.away_team,
        games.home_moneyline,
        games.away_moneyline,
        games.game_date::date as game_date,
        coalesce(games.home_team_rank, 15) as home_team_rank,
        coalesce(home_rest.days_rest, 4) as home_days_rest,
        coalesce(home_team_avg_pts_scored, 112) as home_team_avg_pts_scored,
        coalesce(home_team_avg_pts_scored_opp, 112) as home_team_avg_pts_scored_opp,
        coalesce(home_team_win_pct, 0.50) as home_team_win_pct,
        coalesce(home_team_win_pct_last10, 0.50) as home_team_win_pct_last10,
        coalesce(home_is_top_players, 2)::numeric as home_is_top_players,
        coalesce(home_travel.travel_miles_last_7_days, 0) as home_travel_miles_last_7_days,
        coalesce(home_travel.games_last_7_days, 0) as home_games_last_7_days,
        coalesce(home_travel.is_cross_country_trip, false) as home_is_cross_country_trip,
        coalesce(games.away_team_rank, 15) as away_team_rank,
        coalesce(away_rest.days_rest, 4) as away_days_rest,
        coalesce(away_team_avg_pts_scored, 112) as away_team_avg_pts_scored,
        coalesce(away_team_avg_pts_scored_opp, 112) as away_team_avg_pts_scored_opp,
        coalesce(away_team_win_pct, 0.50) as away_team_win_pct,
        coalesce(away_team_win_pct_last10, 0.50) as away_team_win_pct_last10,
        coalesce(away_is_top_players, 2)::numeric as away_is_top_players,
        coalesce(away_travel.travel_miles_last_7_days, 0) as away_travel_miles_last_7_days,
        coalesce(away_travel.games_last_7_days, 0) as away_games_last_7_days,
        coalesce(away_travel.is_cross_country_trip, false) as away_is_cross_country_trip,
        -- Travel differential: negative = home team traveled more
        coalesce(home_travel.travel_miles_last_7_days, 0) - coalesce(away_travel.travel_miles_last_7_days, 0) as travel_miles_differential,
        outcome
    from games
        left join home_team_avg using (home_team)
        left join home_team_win_pct using (home_team)
        left join home_team_top_players_aggs using (home_team)
        left join away_team_avg using (away_team)
        left join away_team_win_pct using (away_team)
        left join away_team_top_players_aggs using (away_team)
        left join outcomes using (home_team, game_date)
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
)

-- outcome == 1 means home team won,
-- outcome == 0 means away team won
select *
from final
