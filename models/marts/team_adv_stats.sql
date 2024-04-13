with aws_adv_stats_table as (

    select *
    from {{ ref('team_adv_stats_data') }}

),

team_attributes as (

    select *
    from {{ ref('teams') }}
),

prod_adv_stats_table as (

    select *
    from aws_adv_stats_table
        left join team_attributes using (team)
)

select *
from prod_adv_stats_table
