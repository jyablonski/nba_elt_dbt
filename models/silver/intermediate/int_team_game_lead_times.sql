with pbp_data as (
    select
        game_date,
        season_type,
        home_team,
        home_team_full,
        away_team,
        away_team_full,
        game_description,
        time_remaining_final,
        leading_team,
        scoring_team,
        margin_score,
        max_home_lead,
        max_away_lead,
        winning_team,
        losing_team
    from {{ ref('fact_pbp_data') }}
),

-- Calculate time between scoring events
pbp_with_time_diff as (
    select
        *,
        coalesce(
            lag(time_remaining_final) over (
                partition by game_date, game_description
                order by time_remaining_final desc
            ),
            48.0
        ) - time_remaining_final as time_difference,
        lag(leading_team) over (
            partition by game_date, game_description
            order by time_remaining_final desc
        ) as prev_leading_team
    from pbp_data
),

-- Calculate lead changes per game
lead_changes_per_game as (
    select
        game_date,
        game_description,
        sum(
            case
                when leading_team != prev_leading_team
                    and leading_team != 'TIE'
                    and prev_leading_team != 'TIE'
                    and prev_leading_team is not null
                    then 1
                else 0
            end
        ) as lead_changes
    from pbp_with_time_diff
    group by
        game_date,
        game_description
),

-- Unpivot to get one row per team perspective
team_game_plays as (
    select
        pbp_with_time_diff.game_date,
        pbp_with_time_diff.season_type,
        home_team as team,
        home_team_full as team_full,
        away_team as opponent,
        away_team_full as opponent_full,
        'HOME' as home_away,
        pbp_with_time_diff.game_description,
        time_remaining_final,
        time_difference,
        margin_score,
        max_home_lead,
        max_away_lead,
        winning_team,
        losing_team,
        lead_changes_per_game.lead_changes,
        case
            when leading_team = home_team then 'Leading'
            when leading_team = away_team then 'Trailing'
            else 'Tied'
        end as game_state
    from pbp_with_time_diff
        inner join lead_changes_per_game
            on pbp_with_time_diff.game_date = lead_changes_per_game.game_date
            and pbp_with_time_diff.game_description = lead_changes_per_game.game_description

    union all

    select
        pbp_with_time_diff.game_date,
        pbp_with_time_diff.season_type,
        away_team as team,
        away_team_full as team_full,
        home_team as opponent,
        home_team_full as opponent_full,
        'AWAY' as home_away,
        pbp_with_time_diff.game_description,
        time_remaining_final,
        time_difference,
        margin_score * -1 as margin_score,
        max_away_lead,
        max_home_lead,
        winning_team,
        losing_team,
        lead_changes_per_game.lead_changes,
        case
            when leading_team = away_team then 'Leading'
            when leading_team = home_team then 'Trailing'
            else 'Tied'
        end as game_state
    from pbp_with_time_diff
        inner join lead_changes_per_game
            on pbp_with_time_diff.game_date = lead_changes_per_game.game_date
            and pbp_with_time_diff.game_description = lead_changes_per_game.game_description
),

-- Calculate time in each game state
game_state_durations as (
    select
        game_date,
        season_type,
        team,
        team_full,
        opponent,
        opponent_full,
        home_away,
        game_description,
        winning_team,
        losing_team,
        max_home_lead,
        max_away_lead,
        max(lead_changes) as lead_changes,
        sum(case when game_state = 'Leading' then time_difference else 0 end) as minutes_leading,
        sum(case when game_state = 'Trailing' then time_difference else 0 end) as minutes_trailing,
        sum(case when game_state = 'Tied' then time_difference else 0 end) as minutes_tied,
        sum(time_difference) as total_minutes,
        count(*) as total_scoring_plays
    from team_game_plays
    group by
        game_date,
        season_type,
        team,
        team_full,
        opponent,
        opponent_full,
        home_away,
        game_description,
        winning_team,
        losing_team,
        max_home_lead,
        max_away_lead
)

select
    game_date,
    season_type,
    team,
    team_full,
    opponent,
    opponent_full,
    home_away,
    game_description,
    case
        when team = winning_team then 'W'
        when team = losing_team then 'L'
        else 'T'
    end as result,

    -- Time statistics (in minutes)
    round(minutes_leading, 2) as minutes_leading,
    round(minutes_trailing, 2) as minutes_trailing,
    round(minutes_tied, 2) as minutes_tied,
    round(total_minutes, 2) as total_minutes,

    -- Percentage statistics
    round((minutes_leading / nullif(total_minutes, 0)) * 100, 2) as pct_time_leading,
    round((minutes_trailing / nullif(total_minutes, 0)) * 100, 2) as pct_time_trailing,
    round((minutes_tied / nullif(total_minutes, 0)) * 100, 2) as pct_time_tied,

    -- Lead statistics (ensure positive values)
    case
        when home_away = 'HOME' then abs(max_home_lead)
        else abs(max_away_lead)
    end as biggest_lead,

    case
        when home_away = 'HOME' then abs(max_away_lead)
        else abs(max_home_lead)
    end as biggest_deficit,

    -- Lead changes
    lead_changes,

    -- Additional context
    total_scoring_plays,

    -- Derived metrics
    coalesce(minutes_leading > minutes_trailing and team = losing_team, false) as led_most_but_lost,

    coalesce(minutes_trailing > minutes_leading and team = winning_team, false) as trailed_most_but_won,

    current_timestamp as created_at

from game_state_durations
order by
    game_date desc,
    team asc
