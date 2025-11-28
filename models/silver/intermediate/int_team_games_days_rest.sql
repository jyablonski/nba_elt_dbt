-- DAYS REST CALCULATION LOGIC:
-- Days rest is calculated as the number of days between games, minus 1
-- Examples:
--   * Game on Oct 24, then Oct 25 -> Oct 25 record shows 0 days rest (back-to-back)
--   * Game on Oct 28, then Oct 31 -> Oct 31 record shows 2 days rest (29th and 30th off)
-- Special cases:
--   * First game of season: Defaults to 5 days rest
--   * Extended breaks (>5 days): Capped at 5 days rest, flagged with was_over_5_days = true

with schedule_unpivoted as (
    -- unpivot to get one row per team per game
    select
        away_team as team,
        proper_date as game_date
    from {{ ref('fact_schedule_data') }}

    union all

    select
        home_team as team,
        proper_date as game_date
    from {{ ref('fact_schedule_data') }}
),

final as (
    select
        team,
        game_date,
        coalesce(
            game_date - lag(game_date) over (partition by team order by game_date) - 1,
            5
        )::numeric as days_rest_raw,
        case
            when coalesce(
                    game_date - lag(game_date) over (partition by team order by game_date) - 1,
                    5
                )::numeric > 5 then 5
            else coalesce(
                    game_date - lag(game_date) over (partition by team order by game_date) - 1,
                    5
                )::numeric
        end as days_rest,
        coalesce(coalesce(
            game_date - lag(game_date) over (partition by team order by game_date) - 1,
            5
        )::numeric > 5, false) as was_over_5_days,
        rank() over (partition by team order by game_date desc) as rank
    from schedule_unpivoted
)

select *
from final
order by
    team,
    game_date
