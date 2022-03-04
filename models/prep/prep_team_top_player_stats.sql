with my_cte as (
    select
        *
    from {{ ref('prep_top_players_present')}}
),

-- total possible 180 combinations
aggs as (
    select
        team,
        outcome,
        is_top_players,
        count(outcome) as outcome_count
    from my_cte
    group by 1, 2, 3
    order by team
),

aggs_wins_2 as (
    select
        team,
        outcome_count as aggs_wins_2
    from aggs
    where outcome = 'W' and is_top_players = 2
),

aggs_wins_1 as (
    select
        team,
        outcome_count as aggs_wins_1
    from aggs
    where outcome = 'W' and is_top_players = 1
),

aggs_wins_0 as (
    select
        team,
        outcome_count as aggs_wins_0
    from aggs
    where outcome = 'W' and is_top_players = 0
),

aggs_losses_2 as (
    select
        team,
        outcome_count as aggs_losses_2
    from aggs
    where outcome = 'L' and is_top_players = 2
),

aggs_losses_1 as (
    select
        team,
        outcome_count as aggs_losses_1
    from aggs
    where outcome = 'L' and is_top_players = 1
),

aggs_losses_0 as (
    select
        team,
        outcome_count as aggs_losses_0
    from aggs
    where outcome = 'L' and is_top_players = 0
),

final as (
    select
        *
    from aggs_wins_2
    left join aggs_wins_1 using (team)
    left join aggs_wins_0 using (team)
    left join aggs_losses_2 using (team)
    left join aggs_losses_1 using (team)
    left join aggs_losses_0 using (team)

),

-- this is where all of the individual counts are
final2 as (
    select 
        team,
        coalesce(aggs_wins_2, 0) as aggs_wins_2,
        coalesce(aggs_wins_1, 0) as aggs_wins_1,
        coalesce(aggs_wins_0, 0) as aggs_wins_0,
        coalesce(aggs_losses_2, 0) as aggs_losses_2,
        coalesce(aggs_losses_1, 0) as aggs_losses_1,
        coalesce(aggs_losses_0, 0) as aggs_losses_0
    from final
),

-- this is the aggregated info
final3 as (
    select
        team,
        concat(aggs_wins_2, '-', aggs_losses_2)::text as record_2,
        concat(aggs_wins_1, '-', aggs_losses_1)::text as record_1,
        concat(aggs_wins_0, '-', aggs_losses_0)::text as record_0,
        case when aggs_wins_2 > 0 then round(aggs_wins_2::numeric / (aggs_wins_2::numeric + aggs_losses_2::numeric), 3)::numeric
        else 0 end as aggs_win_2_pct,
        case when aggs_wins_1 > 0 then round(aggs_wins_1::numeric / (aggs_wins_1::numeric + aggs_losses_1::numeric), 3)::numeric
        else 0 end as aggs_win_1_pct,
        case when aggs_wins_0 > 0 then round(aggs_wins_0::numeric / (aggs_wins_0::numeric + aggs_losses_0::numeric), 3)::numeric
        else 0 end as aggs_win_0_pct,
        aggs_wins_2 + aggs_losses_2 as tot_games_2_players,
        aggs_wins_1 + aggs_losses_1 as tot_games_1_players,
        aggs_wins_0 + aggs_losses_0 as tot_games_0_players

    from final2
    order by aggs_win_2_pct desc
)

select *
from final3