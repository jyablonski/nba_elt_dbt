/* to do - last 10 games column+ win streaks */

with team_wins as (
    select distinct
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".game_id,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".date,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".outcome,
        
        "jacob_db"."nba_staging"."staging_seed_team_attributes".conference,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".team,
        case when outcome = 'W' then 1
                   else 0 end as outcome_int
    from {{ ref('staging_aws_boxscores_table')}}
    left join
        {{ ref('staging_seed_team_attributes')}} on
            
        "jacob_db"."nba_staging"."staging_seed_team_attributes".team_acronym = "jacob_db"."nba_staging"."staging_aws_boxscores_table".team

),

active_injuries as (
    select
        team_acronym as team,
        team_active_injuries
    from {{ ref('staging_aws_injury_data_table')}}

),

team_counts as (
    select
        team,
        count(distinct(game_id)) as games_played,
        sum(outcome_int) as wins,
        (count(distinct(game_id)) - sum(outcome_int)) as losses
    from team_wins
    group by 1
),

team_attributes as (
    select
        team_acronym as team,
        team as team_full
    from {{ ref('staging_seed_team_attributes')}}
),

pre_final as (
    select distinct
        team_attributes.team_full,
        team_wins.conference,
        team_counts.games_played,
        team_counts.wins,
        team_counts.losses,
        team_wins.team,
        coalesce(active_injuries.team_active_injuries, 0) as active_injuries,
        round(
            (team_counts.wins / (team_counts.wins + team_counts.losses)), 3
        )::numeric as win_percentage
    from team_wins
    left join team_counts using (team)
    left join active_injuries using (team)
    left join team_attributes using (team)

),

final as (
    select
        *,
        case when win_percentage >= 0.5 then 'Above .500'
                   else 'Below .500' end as team_status
    from pre_final
)


select *
from final
