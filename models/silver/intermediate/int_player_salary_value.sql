with current_season as (
    select
        cast(split_part(season, '-', 1) as smallint) as season,
        luxury_tax_threshold
    from {{ source('bronze', 'league_cap_thresholds') }}
    where is_current_season = true
),

league_stats as (
    select
        round(avg(avg_mvp_score)::numeric, 2) as league_avg_mvp_score,
        round(stddev_pop(avg_mvp_score)::numeric, 4) as league_mvp_stddev,
        round(avg(salary)::numeric, 2) as league_avg_salary,
        round(stddev_pop(salary)::numeric, 2) as league_salary_stddev
    from {{ ref('int_contract_value_analysis') }}
),

player_age as (
    select distinct on (player)
        {{ clean_player_names_bbref('player') }}::text as player,
        round(age::numeric, 0)::smallint as age
    from {{ source('bronze', 'bbref_player_adv_stats') }}
    order by
        player asc,
        g desc
),

player_production as (
    select
        player as player_name,
        team,
        avg_mvp_score,
        salary
    from {{ ref('int_contract_value_analysis') }}
),

computed as (
    select
        current_season.season,
        player_production.player_name,
        player_production.team,
        dim_players.pos as position,
        player_age.age,
        round(player_production.salary::numeric, 2) as salary_usd,
        round(player_production.avg_mvp_score::numeric, 2) as avg_mvp_score,
        league_stats.league_avg_mvp_score,
        league_stats.league_mvp_stddev,
        round(
            (
                player_production.avg_mvp_score::numeric
                - league_stats.league_avg_mvp_score
            ) / nullif(league_stats.league_mvp_stddev, 0),
            3
        ) as mvp_z_score,
        round(
            (
                player_production.salary::numeric
                - league_stats.league_avg_salary
            ) / nullif(league_stats.league_salary_stddev, 0),
            3
        ) as salary_z_score,
        team_mvp.team_total_mvp_score,
        round(
            player_production.avg_mvp_score::numeric
            / nullif(team_mvp.team_total_mvp_score, 0),
            4
        ) as pct_of_team_production,
        current_season.luxury_tax_threshold,
        round(
            player_production.salary::numeric
            / nullif(current_season.luxury_tax_threshold, 0),
            4
        ) as pct_of_luxury_tax,
        row_number() over (
            partition by player_production.team
            order by player_production.salary desc, player_production.player_name asc
        )::smallint as salary_rank
    from player_production
        cross join current_season
        cross join league_stats
        inner join {{ ref('int_team_mvp_production_summary') }} as team_mvp
            on player_production.team = team_mvp.team
        left join {{ ref('dim_players') }} as dim_players
            on player_production.player_name = dim_players.player
        left join player_age
            on player_production.player_name = player_age.player
),

final as (
    select
        season,
        player_name,
        team,
        position,
        age,
        salary_usd,
        avg_mvp_score,
        league_avg_mvp_score,
        league_mvp_stddev,
        mvp_z_score,
        salary_z_score,
        round((mvp_z_score - salary_z_score)::numeric, 3) as value_z_score,
        team_total_mvp_score,
        pct_of_team_production,
        luxury_tax_threshold,
        pct_of_luxury_tax,
        round(
            (pct_of_team_production - pct_of_luxury_tax)::numeric,
            4
        ) as production_minus_salary_pct,
        case
            when mvp_z_score - salary_z_score >= 1 then 'surplus'
            when mvp_z_score - salary_z_score <= -1 then 'deficit'
            else 'fair'
        end as value_tier,
        mvp_z_score - salary_z_score < 0 as is_overpaid,
        salary_rank
    from computed
)

select *
from final
order by team asc, salary_rank asc, player_name asc
