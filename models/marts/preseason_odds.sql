with my_cte as (
    select
        team,
        team_full,
        conference,
        predicted_wins,
        predicted_losses,
        projected_wins,
        projected_losses,
        championship_odds,
        (projected_wins - predicted_wins)::numeric as wins_differential,
        concat(predicted_wins, ' - ', predicted_losses) as predicted_stats,
        concat(projected_wins, ' - ', projected_losses) as projected_stats,
        case when (projected_wins - predicted_wins)::numeric > 0 then 'Over' else 'Under' end as over_under
    from {{ ref('prep_standings_table') }}
)

select *
from my_cte
