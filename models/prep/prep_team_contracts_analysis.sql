with my_cte as (
    select
        player,
        salary
    from {{ ref('staging_aws_contracts_table') }}
),

player_gp as (
    select
        player,
        team,
        games_played
    from {{ ref('prep_player_aggs') }}
),

team_gp as (
    select distinct
        team,
        win_percentage,
        games_played as team_games_played
    from {{ ref('prep_standings_table') }}
),

combo as (
    select
        player,
        team,
        salary,
        team_games_played,
        coalesce(games_played, 0) as games_played,
        round(win_percentage, 3) as win_percentage,
        salary * games_played as salary_earned,
        salary * team_games_played as salary_earned_max
    from my_cte
        left join player_gp using (player)
        left join team_gp using (team)
),

team_max_date as (
    select distinct
        team,
        max(date) as date
    from {{ ref('prep_past_schedule_analysis') }}
    group by team
),

team_record as (
    select
        team,
        record
    from {{ ref('prep_past_schedule_analysis') }}
        inner join team_max_date using (team)
),

team_counts as (
    select distinct
        team,
        win_percentage,
        sum(salary_earned) as sum_salary_earned,
        sum(salary_earned_max) as sum_salary_earned_max,
        round((sum(salary_earned) / sum(salary_earned_max)), 3) as team_pct_salary_earned
    from combo
    group by team, win_percentage
    order by team_pct_salary_earned desc
),

final as (
    select distinct *
    from team_counts
        left join team_record using (team)
    where team is not null
)

select *
from final
