with scorers as (
    select 
        distinct player,
        type,
        season_avg_ppg,
        season_ts_percent,
        season_avg_plusminus,
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

player_value as (
    select
        player,
        player_mvp_calc_adj,
        top5_candidates,
        mvp_rank,
        games_missed,
        penalized_games_missed
    from {{ ref('prep_contract_value_analysis') }}
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
        season_avg_plusminus,
        season_ts_percent,
        games_played,
        row_number() over (order by season_avg_ppg desc) as ppg_rank,
        case when row_number() over (order by season_avg_ppg desc) <= 20 then 'Top 20 Scorers' else 'Other' end as top20_scorers,
        v.player_mvp_calc_adj,
        v.games_missed,
        v.penalized_games_missed,
        v.top5_candidates,
        v.mvp_rank
    from scorers
    inner join player_recent_team using (player)
    inner join player_value v using (player)
    order by mvp_rank
)

-- where player = 'Justin Robinson'
select *
from final
order by player_mvp_calc_adj desc
