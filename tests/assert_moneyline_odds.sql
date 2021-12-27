/* this test grabs the latest upcoming day's games and checks to make sure moneyline odds arent null

*/
with latest_date as (
    select
        min(proper_date) as proper_date
    from {{ ref('prep_schedule_table') }}
    where proper_date >= ((current_date)::date)
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