with my_cte as (
    select
        season,
        player_name,
        team,
        position,
        age,
        salary_usd,
        avg_mvp_score,
        league_avg_mvp_score,
        league_mvp_stddev,
        mvp_z_score,
        salary_z_score,
        value_z_score,
        team_total_mvp_score,
        pct_of_team_production,
        luxury_tax_threshold,
        pct_of_luxury_tax,
        production_minus_salary_pct,
        value_tier,
        is_overpaid,
        salary_rank
    from {{ ref('int_player_salary_value') }}
)

select
    *,
    {{ dbt.current_timestamp() }} as updated_at
from my_cte
