-- Test that the great bet value flags are calculated correctly based on known thresholds

with test_cases as (
    select
        'Home favorite slight' as test_case,
        -130 as moneyline,
        0.55 as predicted_win_pct,
        1 as expected_is_great_value
    union all
    select 'Home underdog strong', 200, 0.55, 1
    union all
    select 'Home underdog medium', 170, 0.50, 1
    union all
    select 'Not great value - low win pct', -130, 0.40, 0
    union all
    select 'Not great value - wrong moneyline', -150, 0.55, 0
    union all
    select 'Not great value - medium underdog low pct', 170, 0.45, 0
    union all
    select 'Edge case - exactly at threshold 1', -130, 0.55, 1
    union all
    select 'Edge case - exactly at threshold 2', 200, 0.55, 1
    union all
    select 'Edge case - exactly at threshold 3', 170, 0.50, 1
    union all
    select 'Edge case - just below win pct 1', -130, 0.54, 0
    union all
    select 'Edge case - just below win pct 2', 170, 0.49, 0
),

calculated as (
    select
        test_case,
        moneyline,
        predicted_win_pct,
        expected_is_great_value,
        {{ is_great_bet_value('moneyline', 'predicted_win_pct') }} as actual_is_great_value
    from test_cases
),

failures as (
    select *
    from calculated
    where expected_is_great_value != actual_is_great_value
)

select * from failures