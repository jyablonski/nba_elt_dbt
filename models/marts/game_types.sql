with mov_data as (
    select
        mov::integer,
        season_type,
        game_type
    from {{ ref('prep_recent_games_teams') }}
    where season_type in ('Regular Season', 'Playoffs') -- ignore play-in
),

season_totals as (
    select
        season_type,
        count(*) / 2 as total_games
    from mov_data
    group by season_type
),

-- divide by 2 bc 2 teams play each game; but there is only 1 margin of victory
mov_counts as (
    select
        game_type,
        season_type,
        (count(*) / 2) as n,
        case
            when game_type = 'Clutch Game' then '5 points or less'
            when game_type = '10 pt Game' then 'between 6 - 10 points'
            when game_type = '20 pt Game' then 'between 11 and 20 points'
            else 'more than 20 points'
        end as explanation
    from mov_data
    group by
        game_type,
        season_type
)

select
    mov_counts.*,
    round(100.0 * mov_counts.n / season_totals.total_games, 2) as pct_of_total
from mov_counts
    inner join season_totals
    on mov_counts.season_type = season_totals.season_type
order by
    mov_counts.season_type,
    mov_counts.game_type
