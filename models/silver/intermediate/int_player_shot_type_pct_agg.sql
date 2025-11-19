with player_shot_type_avg as (
    select
        player,
        two_p * 2 as avg_two_pts,
        three_p * 3 as avg_three_pts,
        ft * 1 as avg_ft_pts,
        (two_p * 2) + (three_p * 3) + ft as tot_avg_pts
    from {{ ref('fact_player_stats_data') }}
)

select
    player,
    avg_two_pts,
    avg_three_pts,
    avg_ft_pts,
    tot_avg_pts,
    round(avg_two_pts / nullif(tot_avg_pts, 0), 3) as pct_pts_twos,
    round(avg_three_pts / nullif(tot_avg_pts, 0), 3) as pct_pts_threes,
    round(avg_ft_pts / nullif(tot_avg_pts, 0), 3) as pct_pts_fts
from player_shot_type_avg
