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

pre_final as (
    select 
        distinct(t.team),
        a.team_full,
        t.conference,
        c.games_played,
        c.wins,
        c.losses,
        COALESCE(i.team_active_injuries, 0) as active_injuries,
        (c.wins::numeric / games_played::numeric) as win_percentage
    from team_wins t
    left join team_counts c using (team)
    left join active_injuries i using (team)
    left join team_attributes a using (team)

),

recent_10_wins as (
    select team, sum(outcome_int) as wins_last_10
    from team_wins
    group by team
    limit 10
),

recent_10_losses as (
    select team, date, outcome,
    case when outcome = 'L' then 1 else 0 end as loss_count
    from team_wins
    where outcome = 'L'
    order by date desc
),

recent_10_losses_group as (
    select team, sum(loss_count) as losses_last_10
    from recent_10_losses
    group by team
    limit 10
),

preseason as (
    select 
        team_acronym as team,
        championship_odds, 
        predicted_wins, 
        predicted_losses
    from {{ ref('staging_aws_preseason_odds_table')}}
),

final as (
    select
        *,
        case when win_percentage >= 0.5 then 'Above .500'
        else 'Below .500' end as team_status,
        round((wins / games_played), 0)::numeric * 82 as projected_wins,
        round((losses / games_played), 0)::numeric * 82 as projected_losses
    from pre_final
    left join recent_10_wins using (team)
    left join recent_10_losses_group using (team)
    left join preseason using (team)
)

select *
from final

/*
select *
from final
*/
