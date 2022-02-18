/* this test grabs the latest upcoming day's games and checks to make sure moneyline odds arent null
    everything is default utc, so if i run tests after 7pm local time every fails.
    set where filter to query based on local time (utc - 6 hrs) rather than utc.
    daylight savings will fuckthis up again yeet.

    originally did proper_date >= date, but on 2022-02-18 it grabbed records for 2022-02-24.  
    it should only grab games from the current day we're on
*/
with latest_date as (
    select
        min(proper_date) as proper_date
    from {{ ref('prep_schedule_table') }}
    where proper_date = date({{ dbt_utils.current_timestamp() }} - INTERVAL '6 hour')
),

inactive_dates as (
    select
        date as proper_date,
        is_inactive
    from {{ ref('inactive_dates') }}
),

-- filter out all inactive dates, and only grab records where moneyline odds are null on gamedays so the test fails
final as (
    select 
        *
    from {{ ref('prep_schedule_table') }}
    inner join latest_date using (proper_date)
    left join inactive_dates using (proper_date)
    where (home_moneyline IS NULL OR away_moneyline IS NULL) OR (is_inactive != 1)
)

select *
from final