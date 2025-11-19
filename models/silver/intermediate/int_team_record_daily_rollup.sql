-- this sql is used to generate a snapshot of every team's record for every single day in the regular season.
-- teams dont play every single day, so some cross join stuff is needed to create team / date records for days that they dont play
-- then an array is created with all historical values for the different aggs which gets filtered down to grab the most recent one,
-- and is used to replace the null values from the days that the team's didn't play.
-- this is used to create a snapshot of every team's record for every game day in the nba, so conference ranks can be calculated.

with games_cte as (
    select distinct
        games.team,
        attributes.conference,
        location,
        game_date,
        outcome,
        case when outcome = 'W' then 1 else 0 end as outcome_int
    from {{ ref('fact_boxscores') }} as games
        inner join {{ ref('dim_teams' ) }} as attributes on games.team = attributes.team_acronym
),

-- create 30 records for every team for fkn day a game is played in the nba.
date_cross_join as (
    select distinct
        games_cte.team,
        v2.game_date,
        games_cte.conference
    from games_cte
        cross join games_cte as v2
    where v2.game_date < '2024-04-15'
),

-- turn W, L outcome into integers (0s and 1s that we can count by)
running_total as (
    select
        team,
        conference,
        location,
        game_date,
        outcome,
        sum(outcome_int) over (partition by team order by game_date rows between unbounded preceding and current row) as running_total_wins,
        sum(1) over (partition by team order by game_date rows between unbounded preceding and current row) as running_total_games_played
    from games_cte
),

-- create running totals.  for now, values only exist for days that the team played
ranked_table as (
    select
        *,
        coalesce(running_total_games_played - running_total_wins, 0)::numeric as running_total_losses,
        -- row_number() over (partition by conference, date order by round(
        -- (running_total_wins::numeric / running_total_games_played::numeric), 3)::numeric desc) as rank_raw,
        round((running_total_wins::numeric / running_total_games_played::numeric), 3)::numeric as running_win_pct,
        concat(running_total_wins, '-', coalesce(running_total_games_played - running_total_wins, 0)::numeric) as record_as_of_date
    from running_total
),

-- cant use lag function if you have more than 2+ records of nulls in a row per group
-- https://stackoverflow.com/questions/75175658/fill-null-value-by-previous-value-and-group-by-postgresql
-- https://stackoverflow.com/questions/75462304/postgresql-forward-fill-null-values-with-previous-not-null-value-in-group

-- create a combo table of the cross joined date table + the running total table.
-- array_remove((array_agg( is used to grab every historical record for the specified column
-- it's basically creating a massive array {}.  we then filter it down to the most recent non-null value in the following cte
combo_table as (
    select
        date_cross_join.team,
        date_cross_join.game_date,
        date_cross_join.conference,
        ranked_table.running_total_games_played,
        ranked_table.running_total_wins,
        ranked_table.running_total_losses,
        ranked_table.running_win_pct,
        ranked_table.record_as_of_date,
        array_remove((array_agg(running_total_games_played) over (partition by team order by game_date)), null) as running_total_games_played_arr,
        array_remove((array_agg(running_total_wins) over (partition by team order by game_date)), null) as running_total_wins_arr,
        array_remove((array_agg(running_total_losses) over (partition by team order by game_date)), null) as running_total_losses_arr,
        array_remove((array_agg(running_win_pct) over (partition by team order by game_date)), null) as running_win_pct_arr,
        array_remove((array_agg(record_as_of_date) over (partition by team order by game_date)), null) as record_as_of_date_arr
    from date_cross_join
        left join ranked_table using (team, game_date)
    order by date_cross_join.game_date
),

-- this cte filters the fk out of that shit to return only the most recent value
filtered_arr as (
    select
        team,
        game_date,
        conference,
        running_total_games_played_arr[array_upper(running_total_games_played_arr, 1)] as running_total_games_played,
        running_total_wins_arr[array_upper(running_total_wins_arr, 1)] as running_total_wins,
        running_total_losses_arr[array_upper(running_total_losses_arr, 1)] as running_total_losses,
        running_win_pct_arr[array_upper(running_win_pct_arr, 1)] as running_total_win_pct,
        record_as_of_date_arr[array_upper(record_as_of_date_arr, 1)] as record_as_of_date
    from combo_table
),

-- now every single day we have team win / loss records to properly re-calculate conference rank every day.
final as (
    select
        *,
        row_number() over (partition by conference, game_date order by round(
            (running_total_wins::numeric / running_total_games_played::numeric), 3
        )::numeric desc) as rank_raw
    from filtered_arr
    where running_total_games_played is not null
),

-- some case when updates bc of play in game shit yo
final2 as (
    select
        team,
        game_date,
        conference,
        running_total_games_played,
        running_total_wins,
        running_total_losses,
        running_total_win_pct,
        record_as_of_date,
        case
            when game_date = '2023-04-09' and conference = 'Eastern' and rank_raw = 7 then 8
            when game_date = '2023-04-09' and conference = 'Eastern' and rank_raw = 8 then 9
            when game_date = '2023-04-09' and conference = 'Eastern' and rank_raw = 9 then 7
            when game_date = '2023-04-09' and conference = 'Western' and rank_raw = 5 then 6
            when game_date = '2023-04-09' and conference = 'Western' and rank_raw = 6 then 5
            when game_date = '2023-04-09' and conference = 'Western' and rank_raw = 8 then 9
            when game_date = '2023-04-09' and conference = 'Western' and rank_raw = 9 then 8
            else rank_raw
        end as rank_raw
    from final
)

-- turn the rank into an ordinal value (1 -> 1st, 3 -> 3rd etc)
select
    *,
    {{ generate_ord_numbers('rank_raw') }} as rank
from final2
