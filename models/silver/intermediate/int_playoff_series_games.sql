with playoff_schedule_games as (
    select
        extract(year from game_date)::int as season,
        game_date,
        game_ts,
        home_team_acronym,
        away_team_acronym,
        least(home_team_acronym, away_team_acronym) as team_a,
        greatest(home_team_acronym, away_team_acronym) as team_b
    from {{ ref('int_schedule_table') }}
    where season_type = 'Playoffs'
),

game_results as (
    select
        extract(year from game_date)::int as season,
        game_date,
        max(case when location = 'H' then team when location = 'A' then opponent end) as home_team_acronym,
        max(case when location = 'A' then team when location = 'H' then opponent end) as away_team_acronym,
        least(team, opponent) as team_a,
        greatest(team, opponent) as team_b,
        max(case when outcome = 'W' then team end) as winning_team
    from {{ ref('fact_boxscores') }}
    where season_type = 'Playoffs'
    group by
        extract(year from game_date)::int,
        game_date,
        least(team, opponent),
        greatest(team, opponent)
),

playoff_games as (
    select
        coalesce(schedule_games.season, results.season) as season,
        coalesce(schedule_games.game_date, results.game_date) as game_date,
        coalesce(schedule_games.game_ts, results.game_date::timestamp) as game_ts,
        coalesce(schedule_games.home_team_acronym, results.home_team_acronym) as home_team_acronym,
        coalesce(schedule_games.away_team_acronym, results.away_team_acronym) as away_team_acronym,
        coalesce(schedule_games.team_a, results.team_a) as team_a,
        coalesce(schedule_games.team_b, results.team_b) as team_b,
        results.winning_team
    from playoff_schedule_games as schedule_games
        full outer join game_results as results
            on schedule_games.season = results.season
            and schedule_games.game_date = results.game_date
            and schedule_games.team_a = results.team_a
            and schedule_games.team_b = results.team_b
),

series_start_dates as (
    select
        season,
        team_a,
        team_b,
        min(game_date) as series_start_date
    from playoff_games
    group by
        season,
        team_a,
        team_b
),

series_order as (
    select
        *,
        row_number() over (
            partition by season
            order by series_start_date, team_a, team_b
        ) as series_order
    from series_start_dates
),

series_rounds as (
    select
        *,
        case
            when series_order <= 8 then 1
            when series_order <= 12 then 2
            when series_order <= 14 then 3
            else 4
        end as round_number,
        case
            when series_order <= 8 then 'First Round'
            when series_order <= 12 then 'Conf. Semifinals'
            when series_order <= 14 then 'Conf. Finals'
            else 'NBA Finals'
        end as round_name
    from series_order
),

numbered_games as (
    select
        concat(
            games.season::text,
            '-',
            rounds.round_number::text,
            '-',
            games.team_a,
            '-',
            games.team_b
        ) as series_id,
        games.season,
        rounds.round_number,
        rounds.round_name,
        games.team_a,
        games.team_b,
        games.home_team_acronym,
        games.away_team_acronym,
        games.game_date,
        games.game_ts,
        games.winning_team,
        games.winning_team is not null as is_played,
        row_number() over (
            partition by games.season, rounds.round_number, games.team_a, games.team_b
            order by games.game_date, games.game_ts
        ) as series_game_number
    from playoff_games as games
        inner join series_rounds as rounds
            on games.season = rounds.season
            and games.team_a = rounds.team_a
            and games.team_b = rounds.team_b
),

series_progress as (
    select
        *,
        coalesce(sum(case when winning_team = team_a then 1 else 0 end) over (
            partition by series_id
            order by game_date, game_ts
            rows between unbounded preceding and 1 preceding
        ), 0) as team_a_wins_before_game,
        coalesce(sum(case when winning_team = team_b then 1 else 0 end) over (
            partition by series_id
            order by game_date, game_ts
            rows between unbounded preceding and 1 preceding
        ), 0) as team_b_wins_before_game,
        sum(case when winning_team = team_a then 1 else 0 end) over (
            partition by series_id
            order by game_date, game_ts
            rows between unbounded preceding and current row
        ) as team_a_wins_after_game,
        sum(case when winning_team = team_b then 1 else 0 end) over (
            partition by series_id
            order by game_date, game_ts
            rows between unbounded preceding and current row
        ) as team_b_wins_after_game
    from numbered_games
),

series_state as (
    select
        *,
        case
            when team_a_wins_before_game > team_b_wins_before_game then team_a
            when team_b_wins_before_game > team_a_wins_before_game then team_b
        end as series_leader_before_game,
        greatest(team_a_wins_before_game, team_b_wins_before_game) as leader_wins_before_game,
        least(team_a_wins_before_game, team_b_wins_before_game) as trailer_wins_before_game,
        greatest(team_a_wins_before_game, team_b_wins_before_game) >= 4 as is_series_over_before_game,
        case
            when team_a_wins_after_game > team_b_wins_after_game then team_a
            when team_b_wins_after_game > team_a_wins_after_game then team_b
        end as series_leader_after_game,
        greatest(team_a_wins_after_game, team_b_wins_after_game) as leader_wins_after_game,
        least(team_a_wins_after_game, team_b_wins_after_game) as trailer_wins_after_game,
        greatest(team_a_wins_after_game, team_b_wins_after_game) >= 4 as is_series_over_after_game
    from series_progress
)

select
    *,
    case
        when leader_wins_before_game = 0 and trailer_wins_before_game = 0 then 'Game 1'
        when series_leader_before_game is null then concat('Series tied ', leader_wins_before_game::text, '-', trailer_wins_before_game::text)
        when is_series_over_before_game then concat(series_leader_before_game, ' wins ', leader_wins_before_game::text, '-', trailer_wins_before_game::text)
        else concat(series_leader_before_game, ' leads ', leader_wins_before_game::text, '-', trailer_wins_before_game::text)
    end as series_status_before_game,
    case
        when leader_wins_after_game = 0 and trailer_wins_after_game = 0 then 'Game 1'
        when series_leader_after_game is null then concat('Series tied ', leader_wins_after_game::text, '-', trailer_wins_after_game::text)
        when is_series_over_after_game then concat(series_leader_after_game, ' wins ', leader_wins_after_game::text, '-', trailer_wins_after_game::text)
        else concat(series_leader_after_game, ' leads ', leader_wins_after_game::text, '-', trailer_wins_after_game::text)
    end as series_status_after_game
from series_state
