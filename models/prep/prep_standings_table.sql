/* to do - last 10 games column+ win streaks */

with team_wins as (
    select 
        distinct(b.team),
        b.game_id,
        b.date,
        b.outcome,
        a.conference,
        case when outcome = 'W' then 1
        else 0 end as outcome_int
    from {{ ref('staging_aws_boxscores_table')}} b
    left join {{ ref('staging_seed_team_attributes')}} a on a.team_acronym = b.team

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

final as (
    select 
        distinct(t.team),
        a.team_full,
        t.conference,
        c.games_played,
        c.wins,
        c.losses,
        COALESCE(i.team_active_injuries, 0) as active_injuries
    from team_wins t
    left join team_counts c using (team)
    left join active_injuries i using (team)
    left join team_attributes a using (team)

)


select *
from final
