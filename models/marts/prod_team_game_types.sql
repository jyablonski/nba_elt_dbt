with mov_data as (
    select
        team,
        date,
        game_id,
        outcome,
        mov::integer,
        game_type
    from {{ ref('prep_recent_games_teams')}}
),

mov_counts_w as (
    select
        team,
        outcome,
        count(outcome) as outcome_wins_count
    from mov_data
    where outcome = 'W'
    group by 1, 2
),

mov_counts_l as (
    select
        team,
        outcome,
        count(outcome) as outcome_losses_count
    from mov_data
    where outcome = 'L'
    group by 1, 2
),

mov_counts_clutch_wins as (
    select
        team,
        game_type,
        count(game_type) as clutch_wins_count
    from mov_data
    where game_type = 'Clutch Game' and outcome = 'W'
    group by 1, 2
),

mov_counts_clutch_losses as (
    select
        team,
        game_type,
        count(game_type) as clutch_losses_count
    from mov_data
    where game_type = 'Clutch Game' and outcome = 'L'
    group by 1, 2
),

mov_counts_blowout_wins as (
    select
        team,
        game_type,
        count(game_type) as blowout_wins_count
    from mov_data
    where game_type = 'Blowout Game' and outcome = 'W'
    group by 1, 2
),

mov_counts_blowout_losses as (
    select
        team,
        game_type,
        count(game_type) as blowout_losses_count
    from mov_data
    where game_type = 'Blowout Game' and outcome = 'L'
    group by 1, 2
),

mov_counts_10pt_wins as (
    select
        team,
        game_type,
        count(game_type) as tenpt_wins_count
    from mov_data
    where game_type = '10 pt Game' and outcome = 'W'
    group by 1, 2
),

mov_counts_10pt_losses as (
    select
        team,
        game_type,
        count(game_type) as tenpt_losses_count
    from mov_data
    where game_type = '10 pt Game' and outcome = 'L'
    group by 1, 2
),

final as (
    select 
        distinct team,
        coalesce(mov_counts_w.outcome_wins_count, 0) as outcome_wins_count,
        coalesce(mov_counts_l.outcome_losses_count, 0) as outcome_losses_count,
        coalesce(mov_counts_clutch_wins.clutch_wins_count, 0) as clutch_wins_count,
        coalesce(mov_counts_clutch_losses.clutch_losses_count, 0) as clutch_losses_count,
        coalesce(mov_counts_blowout_wins.blowout_wins_count, 0) as blowout_wins_count,
        coalesce(mov_counts_blowout_losses.blowout_losses_count, 0) as blowout_losses_count,
        coalesce(mov_counts_10pt_wins.tenpt_wins_count, 0) tenpt_wins_count, 
        coalesce(mov_counts_10pt_losses.tenpt_losses_count, 0) as tenpt_losses_count
    from mov_data
    left join mov_counts_w using (team)
    left join mov_counts_l using (team)
    left join mov_counts_clutch_wins using (team)
    left join mov_counts_clutch_losses using (team)
    left join mov_counts_blowout_wins using (team)
    left join mov_counts_blowout_losses using (team)
    left join mov_counts_10pt_wins using (team)
    left join mov_counts_10pt_losses using (team)
)

select *
from final