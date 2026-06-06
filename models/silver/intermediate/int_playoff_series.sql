with series_totals as (
    select
        series_id,
        season,
        round_number,
        round_name,
        team_a,
        team_b,
        count(*) as scheduled_games_count,
        sum(case when is_played then 1 else 0 end) as games_played,
        max(team_a_wins_after_game) as team_a_wins,
        max(team_b_wins_after_game) as team_b_wins
    from {{ ref('int_playoff_series_games') }}
    group by
        series_id,
        season,
        round_number,
        round_name,
        team_a,
        team_b
),

series_state as (
    select
        *,
        case
            when team_a_wins > team_b_wins then team_a
            when team_b_wins > team_a_wins then team_b
        end as series_leader,
        greatest(team_a_wins, team_b_wins) as leader_wins,
        least(team_a_wins, team_b_wins) as trailer_wins,
        greatest(team_a_wins, team_b_wins) >= 4 as is_series_over
    from series_totals
)

select
    series_id,
    season,
    round_number,
    round_name,
    team_a,
    team_b,
    team_a_wins,
    team_b_wins,
    games_played,
    scheduled_games_count,
    series_leader,
    leader_wins,
    trailer_wins,
    case
        when games_played = 0 then 'Game 1'
        when series_leader is null then concat('Series tied ', leader_wins::text, '-', trailer_wins::text)
        when is_series_over then concat(series_leader, ' wins ', leader_wins::text, '-', trailer_wins::text)
        else concat(series_leader, ' leads ', leader_wins::text, '-', trailer_wins::text)
    end as series_status,
    is_series_over
from series_state
