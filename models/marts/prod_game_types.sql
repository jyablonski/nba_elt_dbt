with mov_data as (
    select
        game_id,
        mov::integer,
        type,
        game_type
    from {{ ref('prep_recent_games_teams')}}
    where type in ('Regular Season', 'Playoffs') -- ignore play-in
),

-- divide by 2 bc 2 teams play each game; but there is only 1 margin of victory
mov_counts as (
    select 
        game_type,
        type,
        (count(*)/ 2) as n,
        case when game_type = 'Clutch Game' then '5 points or less'
             when game_type = '10 pt Game' then 'between 6 - 10 points'
             else 'more than 10 points' end as explanation
    from mov_data
    group by 1, 2
)

select *
from mov_counts