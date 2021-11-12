with mov_data as (
    select
        game_id,
        mov::integer,
        game_type
    from {{ ref('prep_recent_games_teams')}}
),

mov_data2 as (
    select 
        game_id,
        mov,
        game_type
    from mov_data
),

mov_counts as (
    select 
        game_type,
        (count(*)/ 2) as n,
        case when game_type = 'Clutch Game' then '5 points or less'
             when game_type = '10 pt Game' then 'between 6 - 10 points'
             else 'more than 10 points' end as explanation
    from mov_data2
    group by 1
)

select *
from mov_counts 