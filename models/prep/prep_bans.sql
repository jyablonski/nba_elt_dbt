-- only tracking regular season metrics as of now
with boxscores as (
    select 
        distinct(team),
        game_id,
        location,
        outcome,
        type,
        case when outcome = 'W' then 1
        else 0 end as outcome_int
    from {{ ref('staging_aws_boxscores_table')}}
    where type = 'Regular Season'
),

tot_games_played as (
    select sum(outcome_int) as games_played,
    'join' as join_col
    from boxscores
    where type = 'Regular Season'
),


league_bans as (
    select location, sum(outcome_int) as tot_wins
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
    select coalesce(min(proper_date), current_date + 1) as min_date,
    'join' as join_col
    from {{ ref('staging_aws_schedule_table')}}
    where proper_date >= current_date
),

upcoming_games as (
    select date::date as date,
    'join' as join_col
    from {{ ref('staging_aws_schedule_table')}}
),

upcoming_games_count as (
  select 
    min_date,
    count(*) as upcoming_games,
    'join' as join_col
  from upcoming_games
  left join upcoming_game_date using (join_col)
  where date = min_date
  group by 1
),

league_average_ppg_teams as (
    select 
        team,
        game_id,
        sum(pts) as sum_pts
    from {{ ref('staging_aws_boxscores_table')}}
    where type = 'Regular Season'
    group by 1, 2
),

league_average_ppg as (
    select round(avg(sum_pts), 2) as avg_pts,
    'join' as join_col
    from league_average_ppg_teams
),

latest_update as (
    select 
        max(scrape_time) as scrape_time,
        'join' as join_col
    from {{ ref('staging_aws_reddit_data_table')}}
),

league_ts as (
    select 
        sum(pts) as sum_pts,
        sum(fga) as sum_fga,
        sum(fta::numeric) as sum_fta
    from {{ ref('staging_aws_boxscores_table')}}
    where type = 'Regular Season'

),

league_ts_2 as (
    select {{ generate_ts_percent('sum_pts', 'sum_fga', 'sum_fta') }} as league_ts_percent,
    'join' as join_col
    from league_ts
),

final as (
    select 
        d.min_date as upcoming_game_date,
        coalesce(g.upcoming_games, 0) as upcoming_games,
        b.location,
        b.tot_wins,
        tg.games_played,
        p.avg_pts,
        round((b.tot_wins::numeric / tg.games_played::numeric), 3)::numeric as win_pct,
        u.scrape_time as scrape_time,
        '112.1'::numeric as last_yr_ppg,
        league_ts_2.league_ts_percent as league_ts_percent
    from league_bans_2 as b 
    left join league_average_ppg as p using (join_col)
    left join tot_games_played as tg using (join_col)
    left join latest_update as u using (join_col)
    left join upcoming_games_count g using (join_col)
    left join upcoming_game_date d using (join_col)
    left join league_ts_2 using (join_col)

)

select *
from final