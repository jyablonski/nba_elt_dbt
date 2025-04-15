with play_in_range as (
    select
        min(start_date) as start_date,
        max(end_date) as end_date
    from {{ source('nba_source', 'play_in_details') }}
),

-- define test cases relative to the Play-In range
test_dates as (
    -- 1 day before Play-In start: should be "Regular Season"
    select
        (start_date - interval '1 day') as game_date,
        'Regular Season' as expected_season_type
    from play_in_range

    union all

    -- Play-In start date (inclusive): should be "Play-In"
    select
        start_date as game_date,
        'Play-In' as expected_season_type
    from play_in_range

    union all

    -- Play-In end date (inclusive): should be "Play-In"
    select
        end_date as game_date,
        'Play-In' as expected_season_type
    from play_in_range

    union all

    -- 1 day after Play-In ends: should be "Playoffs"
    select
        (end_date + interval '1 day') as game_date,
        'Playoffs' as expected_season_type
    from play_in_range
)

-- Compare the macro output with the expected season type
select *
from test_dates
where {{ generate_season_type('game_date') }} != expected_season_type
