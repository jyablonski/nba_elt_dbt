with team_attributes as (

    select *
    from {{ ref('dim_teams') }}

),

aws_adv_stats_table as (

    select *
    from {{ ref('fact_team_adv_stats_data') }}

),

prod_adv_stats_table as (

    select
        team_attributes.*,
        aws_adv_stats_table.scrape_date,
        aws_adv_stats_table.age,
        aws_adv_stats_table.w,
        aws_adv_stats_table.l,
        aws_adv_stats_table.pw,
        aws_adv_stats_table.pl,
        aws_adv_stats_table.mov,
        aws_adv_stats_table.sos,
        aws_adv_stats_table.srs,
        aws_adv_stats_table.ortg,
        aws_adv_stats_table.drtg,
        aws_adv_stats_table.nrtg,
        aws_adv_stats_table.pace,
        aws_adv_stats_table.ftr,
        aws_adv_stats_table.three_p_rate,
        aws_adv_stats_table.ts_percent,
        aws_adv_stats_table.efg_percent,
        aws_adv_stats_table.tov_percent,
        aws_adv_stats_table.orb_percent,
        aws_adv_stats_table.ft_fga,
        aws_adv_stats_table.efg_percent_opp,
        aws_adv_stats_table.tov_percent_opp,
        aws_adv_stats_table.drb_percent_opp,
        aws_adv_stats_table.ft_fga_opp,
        aws_adv_stats_table.nrtg_rank,
        aws_adv_stats_table.ortg_rank,
        aws_adv_stats_table.drtg_rank,
        aws_adv_stats_table.srs_rank,
        aws_adv_stats_table.pace_rank,
        aws_adv_stats_table.ts_percent_rank,
        aws_adv_stats_table.tov_percent_opp_rank,
        aws_adv_stats_table.efg_percent_opp_rank,
        aws_adv_stats_table.ft_fga_opp_rank
    from team_attributes
        left join aws_adv_stats_table
            on team_attributes.team = aws_adv_stats_table.team
)

select *
from prod_adv_stats_table
