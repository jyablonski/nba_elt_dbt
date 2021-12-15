with scorers as (
    select 
        distinct player,
        type,
        season_avg_ppg,
        season_ts_percent,
        player_mvp_calc_avg,
        games_played

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
    select distinct
        s.player,
        s.team,
        s.full_team,
        s.date,
        d.max_date
    from {{ ref('staging_aws_boxscores_table')}} s
    inner join player_recent_date d using (player)
    where s.date = d.max_date
),

final as (
        select distinct player,
        team,
        full_team,
        type,
        season_avg_ppg,
        season_ts_percent,
        player_mvp_calc_avg,
        games_played,
        row_number() over (order by player_mvp_calc_avg desc) as mvp_rank,
        row_number() over (order by season_avg_ppg desc) as ppg_rank,
        case when row_number() over (order by player_mvp_calc_avg desc) <= 5 then 'Top 5 MVP Candidate' else 'Other' end as top5_candidates,
        case when row_number() over (order by season_avg_ppg desc) <= 20 then 'Top 20 Scorers' else 'Other' end as top20_scorers

    from scorers
    inner join player_recent_team using (player)
    order by mvp_rank
)

select *
from final
order by player_mvp_calc_avg desc


