/* this test grabs the latest upcoming day's games and checks to make sure moneyline odds arent null
    everything is default utc, so if i run tests after 7pm local time every fails.
    set where filter to query based on local time (utc - 6 hrs or utc - 5 hrs) rather than utc.
    daylight savings will fuckthis up again yeet.

    originally did game_date >= date, but on 2022-02-18 it grabbed records for 2022-02-24.
    it should only grab games from the current day we're on
*/
with latest_date as (
    select min(game_date) as game_date
    from {{ ref('prep_schedule_table') }}
    where game_date = date({{ dbt.current_timestamp() }} - interval '5 hour')
),

-- updates inactive_dates csv file in data folder for all star break / thanksgiving
inactive_dates as (
    select
        date as game_date,
        is_inactive
    from {{ source('nba_source', 'inactive_dates') }}
),

-- filter out all inactive dates, and only grab records where moneyline odds are null on gamedays so the test fails
final as (
    select *
    from {{ ref('prep_schedule_table') }}
        inner join latest_date using (game_date)
        left join inactive_dates using (game_date)
    where (home_moneyline is null or away_moneyline is null) or (is_inactive != 1)
)

select *
from final
