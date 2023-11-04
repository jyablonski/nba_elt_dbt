with agg_stats as (
    select
        player as player,
        season_type,
        avg(fga::numeric) as avg_fga,
        avg(fta::numeric) as avg_fta,
        avg(pts::numeric) as avg_ppg,
        sum(fga::numeric) as sum_fga,
        sum(fta::numeric) as sum_fta,
        sum(pts::numeric) as sum_pts,
        avg(plusminus::numeric) as avg_plus_minus,
        round(
            avg(
                pts::numeric
            ) + (
                0.5 * avg(plusminus::numeric)
            ) + (
                2 * avg(stl::numeric + blk::numeric)
            ) + (
                0.5 * avg(trb::numeric)
            ) + (1.5 * avg(ast::numeric)) - (1.5 * avg(tov::numeric)),
            1
        ) as avg_mvp_score,
        count(*) as games_played
    from {{ ref('staging_aws_boxscores_incremental_table') }}
    where player is not null and season_type = 'Regular Season'
    group by player, season_type
),

player_most_recent_team as (
    select
        player,
        team
    from {{ ref('prep_player_most_recent_team') }}
),

final as (
    select
        agg_stats.player,
        team,
        season_type,
        avg_fga,
        avg_fta,
        round(avg_ppg, 1) as avg_ppg,
        round(avg_plus_minus, 1) as avg_plus_minus,
        avg_mvp_score,
        {{ generate_ts_percent('sum_pts', 'sum_fga', 'sum_fta::numeric') }} as avg_ts_percent,
        games_played,
        row_number() over (order by avg_ppg desc) as ppg_rank,
        row_number() over (order by avg_mvp_score desc) as mvp_rank,
        case
            when row_number() over (order by avg_ppg desc) <= 20
                then 'Top 20 Scorers'
            else 'Other'
        end as scoring_category
    from agg_stats
        inner join player_most_recent_team on agg_stats.player = player_most_recent_team.player
)

select *
from final
