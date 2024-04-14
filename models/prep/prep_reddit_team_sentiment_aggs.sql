with team_aggs as (
    select
        team,
        game_outcome,
        count(*) as num_comments,
        round(avg(avg_score), 3) as avg_score_agg,
        round(avg(avg_compound), 3) as avg_compound_agg
    from {{ ref('prep_reddit_team_sentiment') }}
    where team not in ('RANDOM FLAIR', 'NBA')
    group by
        team,
        game_outcome
    order by
        team,
        round(avg(avg_score), 3) desc
),

team_aggs_addon1 as (
    select
        *,
        case
            when game_outcome = 'NO GAME' then 1
            when game_outcome = 'W' then 2
            else 3
        end as game_outcome_numeric
    from team_aggs
),

-- comparing sentiment differential on wins vs losses
team_aggs_addon2 as (
    select
        *,
        lag(avg_compound_agg) over (partition by team order by game_outcome_numeric) as prev_compound_agg,
        avg_compound_agg - lag(avg_compound_agg) over (partition by team order by game_outcome_numeric) as compound_diff
    from team_aggs_addon1
    order by avg_compound_agg
)

select *
from team_aggs_addon2
