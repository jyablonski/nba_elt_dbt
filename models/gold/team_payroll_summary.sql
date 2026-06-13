with my_cte as (
    select
        team,
        season,
        total_payroll,
        total_market_value,
        total_surplus,
        roster_count,
        salary_cap,
        luxury_tax_threshold,
        first_apron,
        second_apron,
        pct_of_cap,
        is_above_cap,
        is_above_luxury_tax,
        is_above_first_apron,
        is_above_second_apron
    from {{ ref('int_team_payroll_summary') }}
)

select
    *,
    {{ dbt.current_timestamp() }} as __created_at
from my_cte
