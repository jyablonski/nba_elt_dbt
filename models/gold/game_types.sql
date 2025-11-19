with mov_data as (
    select
        mov::integer,
        season_type,
        game_type
    from {{ ref('int_recent_games_teams') }}
    where season_type in ('Regular Season', 'Playoffs') -- ignore play-in
),

-- Create a scaffold of all possible combinations
game_type_scaffold as (
    select
        'Clutch Game' as game_type,
        '5 points or less' as explanation
    union all
    select
        '10 pt Game',
        'between 6 - 10 points'
    union all
    select
        '20 pt Game',
        'between 11 and 20 points'
    union all
    select
        'Blowout Game',
        'more than 20 points'
),

season_type_scaffold as (
    select 'Regular Season' as season_type
    union all
    select 'Playoffs'
),

all_combinations as (
    select
        game_type_scaffold.game_type,
        season_type_scaffold.season_type,
        game_type_scaffold.explanation
    from game_type_scaffold
        cross join season_type_scaffold
),

season_totals as (
    select
        season_type,
        count(*) / 2 as total_games
    from mov_data
    group by season_type
),

-- Divide by 2 bc 2 teams play each game; but there is only 1 margin of victory
mov_counts as (
    select
        game_type,
        season_type,
        (count(*) / 2) as n
    from mov_data
    group by
        game_type,
        season_type
)

select
    all_combinations.game_type,
    all_combinations.season_type,
    coalesce(mov_counts.n, 0) as n,
    all_combinations.explanation,
    case
        when coalesce(season_totals.total_games, 0) = 0 then 0
        else round(100.0 * coalesce(mov_counts.n, 0) / season_totals.total_games, 2)
    end as pct_of_total
from all_combinations
    left join mov_counts
        on all_combinations.game_type = mov_counts.game_type
            and all_combinations.season_type = mov_counts.season_type
    left join season_totals
        on all_combinations.season_type = season_totals.season_type
order by
    all_combinations.season_type,
    all_combinations.game_type
