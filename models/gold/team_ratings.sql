with team_ratings as (

    select *
    from {{ ref('team_adv_stats') }}

),

final_team_ratings as (
    select
        team_ratings.team,
        dim_teams.team_acronym,
        team_ratings.w as wins,
        team_ratings.l as losses,
        team_ratings.ortg,
        team_ratings.drtg,
        team_ratings.nrtg,
        row_number() over (order by nrtg desc)::integer as nrtg_rank,
        row_number() over (order by drtg)::integer as drtg_rank,
        row_number() over (order by ortg desc)::integer as ortg_rank,
        concat('logos/', lower(dim_teams.team_acronym), '.png') as team_logo
    from team_ratings
        left join {{ ref('dim_teams') }} on team_ratings.team = dim_teams.team
),

final as (
    select
        *,
        {{ generate_ord_numbers('nrtg_rank') }} as nrtg_rank2,
        {{ generate_ord_numbers('drtg_rank') }} as drtg_rank2,
        {{ generate_ord_numbers('ortg_rank') }} as ortg_rank2
    from final_team_ratings
)

select
    team,
    team_acronym,
    wins,
    losses,
    ortg,
    drtg,
    nrtg,
    team_logo,
    nrtg_rank2 as nrtg_rank,
    drtg_rank2 as drtg_rank,
    ortg_rank2 as ortg_rank
from final
