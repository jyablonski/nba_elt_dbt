with team_attributes as (
    select *
    from {{ ref('dim_teams') }}
),

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
        team_attributes.team,
        game_type_scaffold.game_type,
        season_type_scaffold.season_type,
        game_type_scaffold.explanation
    from team_attributes
        cross join game_type_scaffold
        cross join season_type_scaffold
),

mov_data as (
    select
        team,
        mov::integer,
        season_type,
        game_type
    from {{ ref('int_recent_games_teams') }}
    where season_type in ('Regular Season', 'Playoffs') -- ignore play-in
),

season_totals as (
    select
        team,
        season_type,
        count(*) as total_games
    from mov_data
    group by
        team,
        season_type
),

mov_counts as (
    select
        team,
        game_type,
        season_type,
        count(*) as n
    from mov_data
    group by
        team,
        game_type,
        season_type
)

select
    all_combinations.team,
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
        on all_combinations.team = mov_counts.team
            and all_combinations.game_type = mov_counts.game_type
            and all_combinations.season_type = mov_counts.season_type
    left join season_totals
        on all_combinations.team = season_totals.team
            and all_combinations.season_type = season_totals.season_type
order by
    all_combinations.season_type,
    all_combinations.team,
    all_combinations.game_type
