with my_cte as (
    select
        distinct b.player,
        b.team,
        b.games_played,
        b.player_mvp_calc_avg,
        coalesce(c.salary, 1000000) as salary
    from {{ ref('staging_aws_boxscores_table') }} b
    left join {{ ref('staging_aws_contracts_table') }} c using (player)
    where type = 'Regular Season'
),

team_gp as (
    select 
        *
    from {{ ref('prep_team_games_played') }}
),

/* postgres sucks ass you cant reference vars created in the current cte so i have to copy paste the transform code over and over */
prep1 as (
    select 
        *,
        team_games_played - games_played as games_missed,
        round(team_games_played * 0.2, 0)::numeric as games_missed_allowance,
        case when (round(team_games_played * 0.2, 0)::numeric < (team_games_played - games_played)) then
                 abs((round(team_games_played * 0.2, 0)::numeric - (team_games_played - games_played)))
            else 0 end as penalized_games_missed,
        round(games_played::numeric /team_games_played::numeric, 3)::numeric as pct_games_played,
        case when salary >= 30000000 then '$30+ M'
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

prep2 as (
    select 
        round(avg(player_mvp_calc_avg), 2)::numeric as pvm_rank,
        salary_rank
    from prep1
    group by 2

),

prep3 as (
    select
        player,
        player_mvp_calc_avg,
        round(percent_rank() OVER(partition by salary_rank order by player_mvp_calc_avg)::numeric, 3)::numeric as rankingish,
        round(percent_rank() OVER(partition by salary_rank order by player_mvp_calc_avg)::numeric, 3)::numeric * 100 as percentile_rank,
        salary_rank
    from prep1
    order by rankingish desc
),

final as (
    select 
        p1.player,
        p1.salary_rank,
        p1.team,
        p1.games_played,
        p1.player_mvp_calc_avg,
        p1.salary,
        p1.team_games_played,
        p1.games_missed,
        p2.pvm_rank,
        p3.rankingish,
        p3.percentile_rank,
        case when percentile_rank >= 60 and salary >= 30000000 then 'Superstars'
        when percentile_rank >= 90 then 'Great Value'
        when percentile_rank < 90 and percentile_rank >= 20 then 'Normal'
        else 'Bad Value'
        end as color_var
    from prep1 p1
    left join prep2 p2 using (salary_rank)
    left join prep3 p3 using (player)
    order by rankingish desc
)

select *
from final