with injury_data as (
    select
         player,
         team,
         date,
        {{dbt_utils.split_part('description', " ' - ' ", 1)}}        as injury,
        {{dbt_utils.split_part('description', " ' - ' ", 2)}}        as description
    from {{ source('nba_source', 'aws_injury_data_source')}}
),

injury_data2 as (
    select
         *,
            {{dbt_utils.split_part('injury', " ' ' ", 1)}}        as status,
            {{dbt_utils.split_part('injury', " ' ' ", 2)}}        as injury2
    from injury_data
),

team_attributes as (

    select
         team,
        team_acronym
    from {{ ref('staging_seed_team_attributes')}}
),

injury_counts as (
    select
         team,
        count(*) as team_active_injuries
    from injury_data
    group by 1
),

final_stg_injury as (
    select
         injury_data2.player,
        team_attributes.team_acronym,
        injury_data2.team,
        injury_data2.date,
        injury_data2.status,
        injury_data2.description,
        injury_counts.team_active_injuries,
        replace(replace(injury_data2.injury2, '(', ''), ')', '') as injury
    from injury_data2
    left join team_attributes using (team)
    left join injury_counts using (team)

)

select * from final_stg_injury
