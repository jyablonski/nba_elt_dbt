with boxscores as (
    select distinct
        game_id,
        location,
        outcome,
        team,
        case when outcome = 'W' then 1
                   else 0 end as outcome_int
    from {{ ref('staging_aws_boxscores_table')}}
),

tot_games_played as (
    select
         'join' as join_col,
        sum(outcome_int) as games_played
    from boxscores
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
        min(proper_date) as min_date
    from {{ ref('staging_aws_schedule_table')}}
),

upcoming_games as (
    select
         date::date as date,
        'join' as join_col
    from {{ ref('staging_aws_schedule_table')}}
),

upcoming_games_count as (
    select
        min_date,
        'join' as join_col,
        count(*) as upcoming_games
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
    group by 1, 2
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
    from {{ ref('staging_aws_reddit_data_table')}}
),

final as (
    select
        upcoming_games_count.upcoming_games,
        upcoming_games_count.min_date as upcoming_game_date,
        upcoming_games_count.join_col,
        league_bans_2.location,
        league_bans_2.tot_wins,
        tot_games_played.games_played,
        league_average_ppg.avg_pts,
        latest_update.scrape_time as scrape_time,
        league_bans_2.tot_wins / tot_games_played.games_played as win_pct,
        '112.1'::numeric as last_yr_ppg
    from upcoming_games_count
    left join league_bans_2 using (join_col)
    left join league_average_ppg using (join_col)
    left join tot_games_played using (join_col)
    left join latest_update using (join_col)
    left join upcoming_games_count using (join_col)

)

select *
from final
