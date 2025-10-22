-- only tracking regular season metrics as of now
with locations as (
    -- Generate base rows for both locations
    select
        'Home' as location,
        'H' as location_code
    union all
    select
        'Away' as location,
        'A' as location_code
),

boxscores as (
    select distinct
        game_date,
        case
            when location = 'H' then 'Home'
            when location = 'A' then 'Away'
            else location
        end as location,
        outcome,
        team,
        case when outcome = 'W' then 1 else 0 end as outcome_int
    from {{ ref('fact_boxscores') }}
    where season_type = 'Regular Season'
),

tot_games_played as (
    select coalesce(sum(outcome_int), 0) as games_played
    from boxscores
),

league_bans as (
    select
        locations.location,
        coalesce(sum(boxscores.outcome_int), 0) as tot_wins
    from locations
        left join boxscores on locations.location = boxscores.location
    group by locations.location
),

upcoming_game_date as (
    select coalesce(min(proper_date), current_date) as min_date
    from {{ ref('fact_schedule_data') }}
    where proper_date >= current_date
),

upcoming_games_count as (
    select
        upcoming_game_date.min_date,
        count(*) as upcoming_games
    from {{ ref('fact_schedule_data') }}
        cross join upcoming_game_date
    where date::date = upcoming_game_date.min_date
    group by upcoming_game_date.min_date
),

team_sum_pts_per_game as (
    select
        team,
        game_date,
        sum(pts) as sum_pts
    from {{ ref('fact_boxscores') }}
    where season_type = 'Regular Season'
    group by
        team,
        game_date
),

recent_game_date as (
    select max(game_date) as most_recent_game
    from {{ ref('fact_boxscores') }}
),

league_average_ppg as (
    select coalesce(round(avg(sum_pts), 2), 0) as avg_pts
    from team_sum_pts_per_game
),

latest_update as (
    select max(scrape_time) as scrape_time
    from {{ ref('fact_reddit_posts') }}
),

league_ts as (
    select
        sum(pts) as sum_pts,
        sum(fga) as sum_fga,
        sum(fta::numeric) as sum_fta
    from {{ ref('fact_boxscores') }}
    where season_type = 'Regular Season'
),

league_ts_2 as (
    select
        {{ generate_ts_percent('sum_pts', 'sum_fga', 'sum_fta') }} as league_ts_percent
    from league_ts
),

final as (
    select
        upcoming_game_date.min_date as upcoming_game_date,
        league_bans.location,
        league_bans.tot_wins,
        tot_games_played.games_played,
        league_average_ppg.avg_pts,
        case
            when tot_games_played.games_played > 0
                then round((league_bans.tot_wins::numeric / tot_games_played.games_played::numeric), 3)
            else 0
        end as win_pct,
        latest_update.scrape_time,
        113.8::numeric as last_yr_ppg,
        league_ts_2.league_ts_percent,
        recent_game_date.most_recent_game,
        coalesce(upcoming_games_count.upcoming_games, 0) as upcoming_games
    from league_bans
        left join league_average_ppg on 1 = 1
        left join tot_games_played on 1 = 1
        left join latest_update on 1 = 1
        left join upcoming_games_count on 1 = 1
        left join upcoming_game_date on 1 = 1
        left join league_ts_2 on 1 = 1
        left join recent_game_date on 1 = 1
)

select * from final
