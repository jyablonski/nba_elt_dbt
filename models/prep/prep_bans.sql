-- only tracking regular season metrics as of now
with boxscores as (
    select distinct
        game_date,
        location,
        outcome,
        season_type,
        team,
        case
            when outcome = 'W' then 1
            else 0
        end as outcome_int
    from {{ ref('fact_boxscores') }}
    where season_type = 'Regular Season'
),

tot_games_played as (
    select sum(outcome_int) as games_played
    from boxscores
    where season_type = 'Regular Season'
),


league_bans as (
    select
        location,
        sum(outcome_int) as tot_wins
    from boxscores
    group by location
),

league_bans_2 as (
    select
        location,
        tot_wins
    from league_bans
),

upcoming_game_date as (
    select coalesce(min(proper_date), current_date + 1) as min_date
    from {{ ref('fact_schedule_data') }}
    where proper_date >= current_date
),

upcoming_games as (
    select date::date as date
    from {{ ref('fact_schedule_data') }}
),

upcoming_games_count as (
    select
        min_date,
        count(*) as upcoming_games
    from upcoming_games
        left join upcoming_game_date on 1 = 1
    where date = min_date
    group by
        min_date
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
    select round(avg(sum_pts), 2) as avg_pts
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
        league_bans_2.location,
        league_bans_2.tot_wins,
        tot_games_played.games_played,
        league_average_ppg.avg_pts,
        round((league_bans_2.tot_wins::numeric / tot_games_played.games_played::numeric), 3)::numeric as win_pct,
        latest_update.scrape_time,
        '114.2'::numeric as last_yr_ppg,
        league_ts_2.league_ts_percent,
        most_recent_game,
        coalesce(upcoming_games_count.upcoming_games, 0) as upcoming_games
    from league_bans_2
        left join league_average_ppg on 1 = 1
        left join tot_games_played on 1 = 1
        left join latest_update on 1 = 1
        left join upcoming_games_count on 1 = 1
        left join upcoming_game_date on 1 = 1
        left join league_ts_2 on 1 = 1
        left join recent_game_date on 1 = 1

)

select *
from final
