with player_teams as (
    select
        player,
        team
    from {{ ref('prep_player_most_recent_team') }}
),

base_player_stats as (
    select distinct
        prep_player_stats.player,
        prep_player_stats.team,
        prep_player_stats.games_played,
        prep_player_stats.avg_mvp_score,
        coalesce(dim_players.salary, 1000000) as salary
    from {{ ref('prep_player_stats') }}
        left join {{ ref('dim_players') }}
            on prep_player_stats.player = dim_players.player
        inner join player_teams
            on prep_player_stats.player = player_teams.player
    where prep_player_stats.season_type = 'Regular Season'
),

team_games as (
    select
        team,
        team_games_played
    from {{ ref('prep_team_games_played') }}
),

salary_buckets as (
    select
        player,
        team,
        games_played,
        avg_mvp_score,
        salary,
        case
            when salary >= 30000000 then '$30+ M'
            when salary >= 25000000 then '$25-30 M'
            when salary >= 20000000 then '$20-25 M'
            when salary >= 15000000 then '$15-20 M'
            when salary >= 10000000 then '$10-15 M'
            when salary >= 5000000 then '$5-10 M'
            else '< $5 M'
        end as salary_rank
    from base_player_stats
),

games_missed_calc as (
    select
        salary_buckets.*,
        team_games.team_games_played,
        team_games.team_games_played - salary_buckets.games_played as games_missed,
        round(team_games.team_games_played * 0.2, 0) as games_missed_allowance,
        round(salary_buckets.games_played::numeric / team_games.team_games_played::numeric, 3) as pct_games_played
    from salary_buckets
        left join team_games
            on salary_buckets.team = team_games.team
),

penalty_calc as (
    select
        *,
        case
            when games_missed_allowance < games_missed
                then abs(games_missed_allowance - games_missed)
            else 0
        end as penalized_games_missed
    from games_missed_calc
),

adjusted_scores as (
    select
        player,
        team,
        games_played,
        salary,
        salary_rank,
        team_games_played,
        games_missed,
        penalized_games_missed,
        -- Apply penalty floor of 0.75
        greatest(
            1 - (2 * (penalized_games_missed / 100.0)),
            0.75
        ) as adj_penalty_final,
        round(
            avg_mvp_score * greatest(
                1 - (2 * (penalized_games_missed / 100.0)),
                0.75
            ),
            2
        ) as adjusted_avg_mvp_score
    from penalty_calc
),

salary_rank_averages as (
    select
        salary_rank,
        round(avg(adjusted_avg_mvp_score), 2) as pvm_rank
    from adjusted_scores
    group by salary_rank
),

player_percentiles as (
    select
        player,
        salary_rank,
        adjusted_avg_mvp_score,
        adj_penalty_final,
        round(
            percent_rank() over (
                partition by salary_rank
                order by adjusted_avg_mvp_score
            )::numeric,
            3
        ) as rankingish,
        round(
            percent_rank() over (
                partition by salary_rank
                order by adjusted_avg_mvp_score
            )::numeric * 100,
            2
        ) as percentile_rank
    from adjusted_scores
),

final as (
    select
        adjusted_scores.player,
        adjusted_scores.salary_rank,
        adjusted_scores.team,
        adjusted_scores.games_played,
        adjusted_scores.salary,
        adjusted_scores.team_games_played,
        adjusted_scores.games_missed,
        adjusted_scores.penalized_games_missed,
        salary_rank_averages.pvm_rank,
        player_percentiles.rankingish,
        player_percentiles.percentile_rank,
        player_percentiles.adjusted_avg_mvp_score as avg_mvp_score,
        player_percentiles.adj_penalty_final,
        case
            when player_percentiles.percentile_rank >= 50
                and adjusted_scores.salary >= 30000000
                then 'Superstars'
            when player_percentiles.percentile_rank >= 90
                then 'Great Value'
            when player_percentiles.percentile_rank >= 20
                then 'Normal'
            else 'Bad Value'
        end as color_var,
        row_number() over (
            order by player_percentiles.adjusted_avg_mvp_score desc
        ) as mvp_rank,
        case
            when row_number() over (
                    order by player_percentiles.adjusted_avg_mvp_score desc
                ) <= 5
                then 'Top 5 MVP Candidate'
            else 'Other'
        end as is_mvp_candidate
    from adjusted_scores
        left join salary_rank_averages
            on adjusted_scores.salary_rank = salary_rank_averages.salary_rank
        left join player_percentiles
            on adjusted_scores.player = player_percentiles.player
)

select *
from final
order by avg_mvp_score desc
