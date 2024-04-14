with player_teams as (
    select
        player,
        team
    from {{ ref('prep_player_most_recent_team') }}
),

my_cte as (
    select distinct
        prep_player_stats.player,
        prep_player_stats.team,
        games_played,
        avg_mvp_score,
        coalesce(salary, 1000000) as salary
    from {{ ref('prep_player_stats') }}
        left join {{ ref('players') }} using (player)
        inner join player_teams using (player)
),

team_gp as (
    select *
    from {{ ref('prep_team_games_played') }}
),

/* postgres sucks ass you cant reference vars created in the current cte so i have to copy paste the transform code over and over */
prep1 as (
    select
        *,
        round(team_games_played * 0.2, 0)::numeric as games_missed_allowance,
        round(games_played::numeric / team_games_played::numeric, 3)::numeric as pct_games_played,
        team_games_played - games_played as games_missed,
        case
            when (round(team_games_played * 0.2, 0)::numeric < (team_games_played - games_played))
                then
                    abs((round(team_games_played * 0.2, 0)::numeric - (team_games_played - games_played)))
            else 0
        end as penalized_games_missed,
        case
            when salary >= 30000000 then '$30+ M'
            when salary >= 25000000 and salary < 30000000 then '$25-30 M'
            when salary >= 20000000 and salary < 25000000 then '$20-25 M'
            when salary >= 15000000 and salary < 20000000 then '$15-20 M'
            when salary >= 10000000 and salary < 15000000 then '$10-15 M'
            when salary >= 5000000 and salary < 10000000 then '$5-10 M'
            else '< $5 M'
        end as salary_rank
    from my_cte
        left join team_gp using (team)
),

prep1_part1 as (
    select
        *,
        (1 - (2 * (penalized_games_missed / 100))) as adj_penalty,
        case
            when (1 - (2 * (penalized_games_missed / 100))) < 0.75 then 0.75
            else (1 - (2 * (penalized_games_missed / 100)))
        end as adj_penalty_final
    from prep1
),

prep1_part2 as (
    select
        player,
        salary_rank,
        adj_penalty_final,
        round(avg_mvp_score * adj_penalty_final, 2)::numeric as avg_mvp_score
    from prep1_part1
),

prep2 as (
    select
        round(avg(avg_mvp_score), 2)::numeric as pvm_rank,
        salary_rank
    from prep1_part2
    group by salary_rank

),

prep3 as (
    select
        player,
        avg_mvp_score,
        adj_penalty_final,
        round(percent_rank() over (partition by salary_rank order by avg_mvp_score)::numeric, 3)::numeric as rankingish,
        salary_rank,
        round(percent_rank() over (partition by salary_rank order by avg_mvp_score)::numeric, 3)::numeric * 100 as percentile_rank
    from prep1_part2
    order by rankingish desc
),

final as (
    select distinct
        p1.player,
        p1.salary_rank,
        p1.team,
        p1.games_played,
        p1.salary,
        p1.team_games_played,
        p1.games_missed,
        p1.penalized_games_missed,
        p2.pvm_rank,
        p3.rankingish,
        p3.percentile_rank,
        p3.avg_mvp_score,
        p3.adj_penalty_final,
        case
            when percentile_rank >= 50 and salary >= 30000000 then 'Superstars'
            when percentile_rank >= 90 then 'Great Value'
            when percentile_rank < 90 and percentile_rank >= 20 then 'Normal'
            else 'Bad Value'
        end as color_var,
        row_number() over (order by p3.avg_mvp_score desc) as mvp_rank,
        case when row_number() over (order by p3.avg_mvp_score desc) <= 5 then 'Top 5 MVP Candidate' else 'Other' end as is_mvp_candidate
    from prep1 as p1
        left join prep2 as p2 using (salary_rank)
        left join prep3 as p3 using (player)
    order by p3.avg_mvp_score desc
)

select *
from final
