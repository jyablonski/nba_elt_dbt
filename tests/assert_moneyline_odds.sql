/* this test grabs the latest upcoming day's games and checks to make sure moneyline odds arent null
    everything is default utc, so if i run tests after 7pm local time every fails.
    set where filter to query based on local time (utc - 6 hrs) rather than utc.
    daylight savings will fuckthis up again yeet.
*/
with latest_date as (
    select
        min(proper_date) as proper_date
    from {{ ref('prep_schedule_table') }}
    where proper_date >= date({{ dbt_utils.current_timestamp() }} - INTERVAL '6 hour')
),

final as (
    select 
        *
    from {{ ref('prep_schedule_table') }}
    inner join latest_date using (proper_date)
    where home_moneyline IS NULL OR away_moneyline IS NULL
)

select *
from final