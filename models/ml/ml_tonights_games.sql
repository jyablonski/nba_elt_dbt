with games as (
    select
        home_team,
        home_moneyline,
        away_team,
        away_moneyline,
        home_team_acronym,
        away_team_acronym,
        proper_date,
        home_team_rank,
        away_team_rank
    from {{ ref('prep_schedule_table') }}
    where proper_date = date({{ dbt_utils.current_timestamp() }} - INTERVAL '6 hour')
),

outcomes as (
    select distinct
        a.team as home_team,
        b.date as proper_date,
        case when b.outcome = 'W' then 1 else 0 end as outcome
    from {{ ref('staging_aws_boxscores_incremental_table') }} b
    left join {{ ref('staging_seed_team_attributes') }} a on b.team = a.team_acronym
    where b.location = 'H'
),

home_team_avg as (
    select 
        full_team as home_team,
        round(avg(pts_scored), 1)::numeric as home_team_avg_pts_scored,
        round(avg(pts_scored_opp), 1)::numeric as home_team_avg_pts_scored_opp
    from {{ ref('prep_recent_games_teams') }}
    group by 1

),

away_team_avg as (
    select 
        full_team as away_team,
        round(avg(pts_scored), 1)::numeric as away_team_avg_pts_scored,
        round(avg(pts_scored_opp), 1)::numeric as away_team_avg_pts_scored_opp
    from {{ ref('prep_recent_games_teams') }}
    group by 1

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

-- these 2 have to be different for tonights games - pull from injury report
team_top_players as (
    select 
        p.player as player,
        p.team_acronym as team_acronym,
        p.team as team,
        t.rank as player_rank
    from {{ ref('staging_aws_injury_data_table') }} as p
    left join {{ ref('staging_seed_top_players') }} as t using (player)
    where t.rank is not null and p.status != 'Day To Day' -- have to use t.rank here and not the renamed player_rank bc postgres YEET BABY
    -- use status != daytoday bc these players will most likely play anyways, so assume they're healthy.
),

home_team_top_players_aggs as (
    select
        team as home_team,
        2 - count(*) as home_is_top_players
    from team_top_players
    group by 1
),

away_team_top_players_aggs as (
    select
        team as away_team,
        2 - count(*) as away_is_top_players
    from team_top_players
    group by 1
),

-- home days rest away days rest has to come from different methodology than ml_past_games

home_days_rest as (
    select 
        team as home_team,
        date as home_last_played_date
    from {{ ref('prep_team_days_rest') }}
    where rank = 1
),

away_days_rest as (
    select 
        team as away_team,
        date as away_last_played_date
    from {{ ref('prep_team_days_rest') }}
    where rank = 1
),

final as (
    select 
        home_team,
        away_team,
        home_moneyline,
        away_moneyline,
        proper_date::date as proper_date,
        home_team_rank,
        (proper_date - home_last_played_date) - 1 AS home_days_rest,
        home_team_avg_pts_scored,
        home_team_avg_pts_scored_opp,
        home_team_win_pct,
        home_team_win_pct_last10,
        coalesce(home_is_top_players, 2)::numeric as home_is_top_players,  -- if top players missing then they're HEALTHY
        away_team_rank,
        (proper_date - away_last_played_date) - 1 as away_days_rest,
        away_team_avg_pts_scored,
        away_team_avg_pts_scored_opp,
        away_team_win_pct,
        away_team_win_pct_last10,
        coalesce(away_is_top_players, 2)::numeric as away_is_top_players,
        outcome
    from games
    left join home_team_avg using (home_team)
    left join home_team_win_pct using (home_team)
    left join home_team_top_players_aggs using (home_team)
    left join away_team_avg using (away_team)
    left join away_team_win_pct using (away_team)
    left join away_team_top_players_aggs using (away_team)
    left join outcomes using (home_team, proper_date)
    left join home_days_rest using (home_team)
    left join away_days_rest using (away_team)
)

-- outcome == 1 means home team won,
-- outcome == 0 means away team won
select *
from final