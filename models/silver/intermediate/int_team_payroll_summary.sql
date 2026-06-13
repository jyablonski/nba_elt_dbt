with cap_thresholds as (
    select
        season,
        salary_cap,
        luxury_tax_threshold,
        first_apron,
        second_apron
    from {{ source('bronze', 'league_cap_thresholds') }}
    where is_current_season = true
),

team_payroll as (
    select
        team,
        sum(salary) as total_payroll,
        sum(market_value) as total_market_value,
        sum(surplus) as total_surplus,
        count(*) as roster_count
    from {{ ref('int_roster_pay_vs_production') }}
    group by team
)

select
    team_payroll.team,
    cap_thresholds.season,
    team_payroll.total_payroll,
    team_payroll.total_market_value,
    team_payroll.total_surplus,
    team_payroll.roster_count,
    cap_thresholds.salary_cap,
    cap_thresholds.luxury_tax_threshold,
    cap_thresholds.first_apron,
    cap_thresholds.second_apron,
    round(team_payroll.total_payroll / cap_thresholds.salary_cap::numeric, 3) as pct_of_cap,
    team_payroll.total_payroll > cap_thresholds.salary_cap as is_above_cap,
    team_payroll.total_payroll > cap_thresholds.luxury_tax_threshold as is_above_luxury_tax,
    team_payroll.total_payroll > cap_thresholds.first_apron as is_above_first_apron,
    team_payroll.total_payroll > cap_thresholds.second_apron as is_above_second_apron
from team_payroll
    cross join cap_thresholds
order by team_payroll.total_payroll desc
