-- only tracking regular season metrics as of now
with boxscores as (
    select distinct
        location,
        outcome,
        season_type,
        (team),
        case
            when outcome = 'W' then 1
            else 0
        end as outcome_int
    from {{ ref('boxscores') }}
    where season_type = 'Regular Season'
),

tot_games_played as (
    select
        'join' as join_col,
        sum(outcome_int) as games_played
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
        tot_wins,
        'join' as join_col
    from league_bans
),

upcoming_game_date as (
    select
        'join' as join_col,
        coalesce(min(proper_date), current_date + 1) as min_date
    from {{ ref('schedule_data') }}
    where proper_date >= current_date
),

upcoming_games as (
    select
        date::date as date,
        'join' as join_col
    from {{ ref('schedule_data') }}
),

upcoming_games_count as (
    select
        min_date,
        count(*) as upcoming_games,
        'join' as join_col
    from upcoming_games
        left join upcoming_game_date using (join_col)
    where date = min_date
    group by
        min_date,
        join_col
),

league_average_ppg_teams as (
    select
        team,
        sum(pts) as sum_pts
    from {{ ref('boxscores') }}
    where season_type = 'Regular Season'
    group by
        team
),

recent_game_date as (
    select
        'join' as join_col,
        max(game_date) as most_recent_game
    from {{ ref('boxscores') }}
),

league_average_ppg as (
    select
        'join' as join_col,
        round(avg(sum_pts), 2) as avg_pts
    from league_average_ppg_teams
),

latest_update as (
    select
        'join' as join_col,
        max(scrape_time) as scrape_time
    from {{ ref('reddit_posts') }}
),

league_ts as (
    select
        sum(pts) as sum_pts,
        sum(fga) as sum_fga,
        sum(fta::numeric) as sum_fta
    from {{ ref('boxscores') }}
    where season_type = 'Regular Season'

),

league_ts_2 as (
    select
        {{ generate_ts_percent('sum_pts', 'sum_fga', 'sum_fta') }} as league_ts_percent,
        'join' as join_col
    from league_ts
),

final as (
    select
        d.min_date as upcoming_game_date,
        b.location,
        b.tot_wins,
        tg.games_played,
        p.avg_pts,
        round((b.tot_wins::numeric / tg.games_played::numeric), 3)::numeric as win_pct,
        u.scrape_time as scrape_time,
        '114.7'::numeric as last_yr_ppg,
        league_ts_2.league_ts_percent as league_ts_percent,
        most_recent_game,
        coalesce(g.upcoming_games, 0) as upcoming_games
    from league_bans_2 as b
        left join league_average_ppg as p using (join_col)
        left join tot_games_played as tg using (join_col)
        left join latest_update as u using (join_col)
        left join upcoming_games_count as g using (join_col)
        left join upcoming_game_date as d using (join_col)
        left join league_ts_2 using (join_col)
        left join recent_game_date using (join_col)

)

select *
from final
