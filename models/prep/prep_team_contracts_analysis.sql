with my_cte as (
    select
        *
    from {{ ref('staging_aws_contracts_table')}}
),

player_gp as (
    select 
        distinct player,
        games_played
    from {{ ref('staging_aws_boxscores_table')}}
),

team_gp as (
    select 
        distinct team,
        games_played as team_games_played
    from {{ ref('prep_standings_table')}}
),

combo as (
    select 
        player,
        team,
        salary,
        coalesce(games_played, 0) as games_played,
        team_games_played,
        salary * games_played as salary_earned,
        salary * team_games_played as salary_earned_max
    from my_cte
    left join player_gp using (player)
    left join team_gp using (team)
),

team_counts as (
    select
        distinct team,
        sum(salary_earned) as sum_salary_earned,
        sum(salary_earned_max) as sum_salary_earned_max,
        round((sum(salary_earned) / sum(salary_earned_max)), 3) as team_pct_salary_earned
    from combo
    group by team
    order by team_pct_salary_earned desc
)

select *
from team_counts