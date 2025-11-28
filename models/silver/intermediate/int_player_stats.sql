with agg_stats as (
    select
        player,
        season_type,
        round(avg(fga::numeric), 2) as avg_fga,
        round(avg(fta::numeric), 2) as avg_fta,
        round(avg(pts::numeric), 2) as avg_ppg,
        sum(fga::numeric) as sum_fga,
        sum(fta::numeric) as sum_fta,
        sum(pts::numeric) as sum_pts,
        avg(plus_minus::numeric) as avg_plus_minus,
        case
            when sum(pts::numeric) = 0 and sum(fga::numeric) = 0 and sum(fta::numeric) = 0 then null
            else round(sum(pts::numeric) / (2 * (sum(fga::numeric) + (sum(fta::numeric) * 0.44))), 3)
        end as avg_ts_percent,
        round(
            avg(
                pts::numeric
            ) + (
                0.5 * avg(plus_minus::numeric)
            ) + (
                2 * avg(stl::numeric + blk::numeric)
            ) + (
                0.5 * avg(trb::numeric)
            ) + (1.5 * avg(ast::numeric)) - (1.5 * avg(tov::numeric)),
            1
        ) as avg_mvp_score,
        count(*) as games_played
    from {{ ref('fact_boxscores') }}
    where
        player is not null
        and season_type in ('Regular Season', 'Playoffs')
    group by
        player,
        season_type
),

player_most_recent_team as (
    select
        player,
        team
    from {{ ref('int_player_most_recent_team') }}
),

ranks as (
    select
        player,
        row_number() over (order by avg_ppg desc) as ppg_rank,
        row_number() over (order by avg_mvp_score desc) as mvp_rank
    from agg_stats
    where season_type = 'Regular Season'
),

final as (
    select
        agg_stats.player,
        player_most_recent_team.team,
        agg_stats.season_type,
        agg_stats.avg_fga,
        agg_stats.avg_fta,
        round(agg_stats.avg_ppg, 1) as avg_ppg,
        round(agg_stats.avg_plus_minus, 1) as avg_plus_minus,
        agg_stats.avg_mvp_score,
        agg_stats.avg_ts_percent,
        agg_stats.games_played,
        ranks.ppg_rank,
        ranks.mvp_rank,

        -- ML Feature for Star Power
        case
            when ranks.mvp_rank <= 5 then 2   -- Superstars (MVP Candidates)
            when ranks.mvp_rank <= 25 then 1  -- All-Star Caliber Players
            else 0                            -- Role Players
        end as star_tier_score,

        case
            when ranks.ppg_rank <= 20 then 'Top 20 Scorers'
            else 'Other'
        end as scoring_category

    from agg_stats
        inner join player_most_recent_team on agg_stats.player = player_most_recent_team.player
        inner join ranks on agg_stats.player = ranks.player
)

select *
from final
