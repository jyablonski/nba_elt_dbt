with scorers as (
    select 
        distinct player,
        type,
        season_avg_ppg,
        season_ts_percent,
        player_mvp_calc_avg,
        games_played,
        mvp_rank,
        ppg_rank,
        top5_candidates,
        top20_scorers


    from {{ ref('staging_aws_boxscores_table')}}
),

player_recent_date as (
    select 
    player,
    max(date) as max_date
    from {{ ref('staging_aws_boxscores_table')}}
    group by 1
),

player_recent_team as (
    select 
        s.player,
        s.team,
        s.date,
        d.max_date
    from {{ ref('staging_aws_boxscores_table')}} s
    inner join player_recent_date d using (player)
    where s.date = d.max_date
),

final as (
        select distinct player,
        team,
        type,
        season_avg_ppg,
        season_ts_percent,
        player_mvp_calc_avg,
        games_played,
        mvp_rank,
        ppg_rank,
        top5_candidates,
        top20_scorers

    from scorers
    inner join player_recent_team using (player)
    order by mvp_rank
)

select *
from final 